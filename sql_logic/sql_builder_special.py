# --- START OF FILE sql_builder_special.py ---
import psycopg2
import psycopg2.sql as pgsql
import time # For unique temp table names
import traceback
from utils import validate_column_name
from app_config import SQL_AGGREGATES, AGGREGATE_RESULT_TYPES, DEFAULT_TEXT_VALUE_COLUMN # 导入新的配置

from typing import List, Tuple, Dict, Any, Optional

def build_special_data_sql(
    target_cohort_table_name: str,
    base_new_column_name: str,
    panel_specific_config: Dict[str, Any],
    for_execution: bool = False,
    preview_limit: int = 100
) -> Tuple[Optional[Any], Optional[str], Optional[List[Any]], List[Tuple[str, str]]]:
    """
    构建用于专项数据提取的SQL查询或执行步骤。
    """
    generated_column_details_for_preview = [] # [(col_name_str, col_type_display_str), ...]

    # --- 1. 解析和验证 panel_specific_config ---
    source_event_table = panel_specific_config.get("source_event_table")
    id_col_in_event_table = panel_specific_config.get("item_id_column_in_event_table")
    # value_column_to_extract 决定了是提取数值还是文本，以及是否进行CAST
    value_column_name_from_panel = panel_specific_config.get("value_column_to_extract") # "valuenum", "value", or None
    time_col_for_window = panel_specific_config.get("time_column_in_event_table")
    selected_item_ids = panel_specific_config.get("selected_item_ids", [])

    aggregation_methods: Optional[Dict[str, bool]] = panel_specific_config.get("aggregation_methods")
    event_outputs: Optional[Dict[str, bool]] = panel_specific_config.get("event_outputs")
    current_time_window_text = panel_specific_config.get("time_window_text")
    cte_join_override = panel_specific_config.get("cte_join_on_cohort_override")

    if not all([source_event_table, id_col_in_event_table, current_time_window_text]): # selected_item_ids 可以为空
        return None, "面板配置信息不完整 (源表,项目ID列,时间窗口)。", [], generated_column_details_for_preview
    if not selected_item_ids: # 如果没有选择任何 item_id，也认为配置不完整
        return None, "未选择任何要提取的项目ID。", [], generated_column_details_for_preview
    if not (aggregation_methods and any(aggregation_methods.values())) and \
       not (event_outputs and any(event_outputs.values())):
        return None, "未选择任何聚合方法或事件输出类型。", [], generated_column_details_for_preview

    # --- 2. 定义SQL构建中常用的标识符 ---
    try:
        schema_name, table_only_name = target_cohort_table_name.split('.')
    except ValueError:
        return None, f"目标队列表名 '{target_cohort_table_name}' 格式不正确 (应为 schema.table)。", [], []
        
    target_table_ident = pgsql.Identifier(schema_name, table_only_name)
    cohort_alias = pgsql.Identifier("cohort")
    event_alias = pgsql.Identifier("evt")
    event_admission_alias = pgsql.Identifier("adm_evt") # 用于"住院以前"的 admissions 表别名
    md_alias = pgsql.Identifier("md") # MergedData CTE 别名
    target_alias = pgsql.Identifier("target") # 用于 UPDATE 语句中的目标表别名

    # --- 3. 构建 FilteredEvents CTE 的参数和条件 ---
    params_for_cte = []
    item_id_filter_on_event_table_parts = []
    event_table_item_id_col_ident = pgsql.Identifier(id_col_in_event_table)

    if len(selected_item_ids) == 1:
        item_id_filter_on_event_table_parts.append(pgsql.SQL("{}.{} = %s").format(event_alias, event_table_item_id_col_ident))
        params_for_cte.append(selected_item_ids[0])
    elif len(selected_item_ids) > 1:
        item_id_filter_on_event_table_parts.append(pgsql.SQL("{}.{} IN %s").format(event_alias, event_table_item_id_col_ident))
        params_for_cte.append(tuple(selected_item_ids))
    else: # selected_item_ids 为空，这不应该发生，因为上面有检查
        return None, "内部错误: selected_item_ids 为空但通过了早期检查。", [], []


    time_filter_conditions_sql_parts = []
    # 从 cohort 表中获取的时间戳列，带上别名
    cohort_icu_intime = pgsql.SQL("{}.icu_intime").format(cohort_alias)
    cohort_icu_outtime = pgsql.SQL("{}.icu_outtime").format(cohort_alias)
    cohort_admittime = pgsql.SQL("{}.admittime").format(cohort_alias)
    cohort_dischtime = pgsql.SQL("{}.dischtime").format(cohort_alias)
    actual_event_time_col_ident = pgsql.Identifier(time_col_for_window) if time_col_for_window else None

    from_join_clause_for_cte = pgsql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.hadm_id = {coh_alias}.hadm_id") \
                               .format(event_table=pgsql.SQL(source_event_table), evt_alias=event_alias,
                                       cohort_table=target_table_ident, coh_alias=cohort_alias)
    if source_event_table == "mimiciv_icu.chartevents":
        from_join_clause_for_cte = pgsql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.stay_id = {coh_alias}.stay_id") \
                                   .format(event_table=pgsql.SQL(source_event_table), evt_alias=event_alias,
                                           cohort_table=target_table_ident, coh_alias=cohort_alias)
    if cte_join_override: # 应用面板提供的JOIN覆盖
        try:
            from_join_clause_for_cte = cte_join_override.format(
                event_table=pgsql.SQL(source_event_table), evt_alias=event_alias,
                adm_evt=event_admission_alias,
                cohort_table=target_table_ident, coh_alias=cohort_alias
            )
        except KeyError as e:
             return None, f"JOIN覆盖SQL模板错误: 缺少占位符 {e}", params_for_cte, []


    # 时间窗口条件构建
    is_value_source = bool(value_column_name_from_panel) # 如果有值列，则为True
    is_text_extraction = (value_column_name_from_panel == DEFAULT_TEXT_VALUE_COLUMN)

    if is_value_source: # 如 labevents, chartevents
        if not actual_event_time_col_ident: return None, f"值类型提取 ({source_event_table}) 需要时间列进行窗口化。", params_for_cte, []
        if current_time_window_text == "ICU入住后24小时": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '24 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
        elif current_time_window_text == "ICU入住后48小时": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '48 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
        elif current_time_window_text == "整个ICU期间": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime, end_ts=cohort_icu_outtime))
        elif current_time_window_text == "整个住院期间": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_admittime, end_ts=cohort_dischtime))
        # else: 未知时间窗口，暂不添加条件或报错
    else: # 事件型数据源 (如 prescriptions, procedures_icd, diagnoses_icd)
        if current_time_window_text == "住院以前 (既往史)":
            if not cte_join_override: # 对于“住院以前”，必须有JOIN覆盖
                return None, f"事件类型提取 ({source_event_table}) 选择“住院以前”时，必须提供JOIN覆盖逻辑。", params_for_cte, []
            time_filter_conditions_sql_parts.append(pgsql.SQL("{adm_evt}.admittime < {compare_ts}").format(adm_evt=event_admission_alias, compare_ts=cohort_admittime))
        elif actual_event_time_col_ident: # 其他事件型时间窗口，如果事件表有时间列
            if current_time_window_text == "整个住院期间 (当前入院)":
                time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_admittime, end_ts=cohort_dischtime))
            elif current_time_window_text == "整个ICU期间 (当前入院)":
                time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime, end_ts=cohort_icu_outtime))
        # else: 事件型数据，但没有事件时间列（如diagnoses_icd），其时间上下文由JOIN的hadm_id限定，不在这里添加额外时间过滤


    # --- 4. 构建 FilteredEvents CTE ---
    select_event_cols_defs = [
        pgsql.SQL("{}.subject_id AS subject_id").format(cohort_alias),
        pgsql.SQL("{}.hadm_id AS hadm_id_cohort").format(cohort_alias) # Group by this hadm_id
    ]
    # 根据面板配置中指定的列名来选择事件表中的值列
    event_value_col_for_select = pgsql.Identifier(value_column_name_from_panel) if value_column_name_from_panel else None

    if event_value_col_for_select:
        select_event_cols_defs.append(pgsql.SQL("{}.{} AS event_value").format(event_alias, event_value_col_for_select))
    if actual_event_time_col_ident:
        select_event_cols_defs.append(pgsql.SQL("{}.{} AS event_time").format(event_alias, actual_event_time_col_ident))

    all_where_conditions_sql_parts = item_id_filter_on_event_table_parts + time_filter_conditions_sql_parts
    filtered_events_cte_sql = pgsql.SQL(
        "FilteredEvents AS (SELECT DISTINCT {select_list} {from_join_clause} WHERE {conditions})"
    ).format(
        select_list=pgsql.SQL(', ').join(select_event_cols_defs),
        from_join_clause=from_join_clause_for_cte,
        conditions=pgsql.SQL(' AND ').join(all_where_conditions_sql_parts) if all_where_conditions_sql_parts else pgsql.SQL("TRUE")
    )

    # --- 5. 构建聚合逻辑 ---
    selected_methods_details = [] # (final_col_name_str, final_col_ident, agg_sql_template_str, col_type_sql_obj)
    type_map_display = { "NUMERIC": "Numeric", "INTEGER": "Integer", "BOOLEAN": "Boolean", "TEXT": "Text", "DOUBLE PRECISION": "Numeric (Decimal)" }

    # 处理 aggregation_methods (来自值源，如 chartevents, labevents)
    if aggregation_methods and any(aggregation_methods.values()):
        if not value_column_name_from_panel: # 值聚合必须有值列
            return None, "值聚合方法被选择，但未指定要聚合的值列。", params_for_cte, []

        for method_key, is_selected in aggregation_methods.items():
            if is_selected:
                sql_template = SQL_AGGREGATES.get(method_key)
                if not sql_template:
                    print(f"警告: 未在 SQL_AGGREGATES 中找到方法 '{method_key}' 的模板。跳过此方法。")
                    continue

                # 确定列类型
                col_type_str_raw = AGGREGATE_RESULT_TYPES.get(method_key)
                if not col_type_str_raw:
                    print(f"警告: 未在 AGGREGATE_RESULT_TYPES 中找到方法 '{method_key}' 的结果类型。默认为 NUMERIC。")
                    col_type_str_raw = "NUMERIC" # Fallback type

                # 特殊处理：如果原始数据是文本，MIN/MAX/FIRST_VALUE/LAST_VALUE 的结果类型应为 TEXT
                if is_text_extraction and method_key in ["MIN", "MAX", "FIRST_VALUE", "LAST_VALUE"]:
                    col_type_str_raw = "TEXT"
                
                col_type_sql_obj = pgsql.SQL(col_type_str_raw)
                actual_suffix = method_key.lower()
                final_col_name_str = f"{base_new_column_name}_{actual_suffix}"
                is_valid, err = validate_column_name(final_col_name_str)
                if not is_valid: return None, f"生成的列名 '{final_col_name_str}' 无效: {err}", params_for_cte, []

                selected_methods_details.append((final_col_name_str, pgsql.Identifier(final_col_name_str), sql_template, col_type_sql_obj))
                generated_column_details_for_preview.append((final_col_name_str, type_map_display.get(col_type_str_raw.upper(), col_type_str_raw)))

    # 处理 event_outputs (来自事件源，如 prescriptions, procedures_icd)
    elif event_outputs and any(event_outputs.values()):
        # 这些是固定的，不依赖 SQL_AGGREGATES
        event_method_configs = {
            "exists": ("TRUE", pgsql.SQL("BOOLEAN")), # 模板, 类型SQL对象
            "countevt": ("COUNT(*)", pgsql.SQL("INTEGER"))
        }
        for method_key, is_selected in event_outputs.items():
            if is_selected:
                if method_key not in event_method_configs:
                    print(f"警告: 未知的事件输出类型 '{method_key}'。跳过。")
                    continue
                
                agg_template, col_type_sql_obj = event_method_configs[method_key]
                col_type_str_raw = str(col_type_sql_obj).replace('SQL(', '').replace(')', '').strip("'\"") # 获取类型字符串

                final_col_name_str = f"{base_new_column_name}_{method_key.lower()}"
                is_valid, err = validate_column_name(final_col_name_str)
                if not is_valid: return None, f"生成的列名 '{final_col_name_str}' 无效: {err}", params_for_cte, []

                selected_methods_details.append((final_col_name_str, pgsql.Identifier(final_col_name_str), agg_template, col_type_sql_obj))
                generated_column_details_for_preview.append((final_col_name_str, type_map_display.get(col_type_str_raw.upper(), col_type_str_raw)))

    if not selected_methods_details: # 如果没有任何方法被成功构建
        return None, "未能构建任何有效的提取列。", params_for_cte, generated_column_details_for_preview

    # 构建主聚合查询部分
    aggregated_columns_sql_list = []
    # FilteredEvents CTE 中的列别名
    fe_val_ident_in_cte = pgsql.Identifier("event_value") # 来自 SELECT ... AS event_value
    fe_time_ident_in_cte = pgsql.Identifier("event_time") # 来自 SELECT ... AS event_time
    cte_hadm_id_for_grouping = pgsql.Identifier("hadm_id_cohort") # 来自 SELECT cohort.hadm_id AS hadm_id_cohort

    for _, final_col_ident, agg_sql_template_str, _ in selected_methods_details:
        # 准备替换模板中的 {val_col} 和 {time_col}
        val_col_replacement = fe_val_ident_in_cte
        if is_text_extraction and "{val_col}" in agg_sql_template_str:
            # 如果是文本提取，并且模板需要值列，则对值列进行CAST
            # 除非模板本身已经处理了CAST (例如某些文本专用模板)
            # 一个简单的检查：如果模板包含 CAST( 或LOWER( 或UPPER( 等文本函数，可能它已处理
            if not any(func in agg_sql_template_str.upper() for func in ["CAST(", "LOWER(", "UPPER("]):
                 val_col_replacement = pgsql.SQL("CAST({} AS TEXT)").format(fe_val_ident_in_cte)

        # 替换占位符
        formatted_agg_expr_str = agg_sql_template_str
        if "{val_col}" in formatted_agg_expr_str:
            if not value_column_name_from_panel: # 如果模板需要值列，但我们没有（例如，事件输出不应该到这里）
                return None, f"聚合模板 '{agg_sql_template_str}' 需要值列，但配置未提供。", params_for_cte, []
            formatted_agg_expr_str = formatted_agg_expr_str.replace("{val_col}", val_col_replacement.string if isinstance(val_col_replacement, pgsql.Identifier) else str(val_col_replacement))
        
        if "{time_col}" in formatted_agg_expr_str:
            if not time_col_for_window: # 如果模板需要时间列，但我们没有
                return None, f"聚合模板 '{agg_sql_template_str}' 需要时间列，但配置未提供。", params_for_cte, []
            formatted_agg_expr_str = formatted_agg_expr_str.replace("{time_col}", fe_time_ident_in_cte.string)
        
        sql_expr = pgsql.SQL(formatted_agg_expr_str)
        aggregated_columns_sql_list.append(pgsql.SQL("{} AS {}").format(sql_expr, final_col_ident))

    # FilteredEvents 中的 hadm_id_cohort 用于分组
    group_by_clause_sql = pgsql.SQL("GROUP BY {}").format(cte_hadm_id_for_grouping)
    main_aggregation_select_sql = pgsql.SQL("SELECT {hadm_col}, {agg_cols} FROM FilteredEvents {group_by}").format(
        hadm_col=cte_hadm_id_for_grouping,
        agg_cols=pgsql.SQL(', ').join(aggregated_columns_sql_list),
        group_by=group_by_clause_sql
    )
    data_generation_query_part = pgsql.SQL("WITH {filtered_cte} {main_agg_select}").format(
        filtered_cte=filtered_events_cte_sql, main_agg_select=main_aggregation_select_sql)

    # --- 6. 根据 for_execution 返回执行步骤或预览SQL ---
    if for_execution:
        alter_clauses = []
        for _, final_col_ident, _, col_type_sql_obj in selected_methods_details:
            alter_clauses.append(pgsql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(final_col_ident, col_type_sql_obj))
        alter_sql = pgsql.SQL("ALTER TABLE {target_table} ").format(target_table=target_table_ident) + pgsql.SQL(', ').join(alter_clauses) + pgsql.SQL(";")

        temp_table_data_name_str = f"temp_merge_data_{base_new_column_name.lower().replace('-', '_')}_{int(time.time()) % 100000}"
        if len(temp_table_data_name_str) > 63: temp_table_data_name_str = temp_table_data_name_str[:63]
        temp_table_data_ident = pgsql.Identifier(temp_table_data_name_str)
        create_temp_table_sql = pgsql.SQL("CREATE TEMPORARY TABLE {temp_table} AS ({data_gen_query});").format(temp_table=temp_table_data_ident, data_gen_query=data_generation_query_part)

        update_set_clauses = []
        for _, final_col_ident, _, _ in selected_methods_details:
            update_set_clauses.append(pgsql.SQL("{col_to_set} = {tmp_alias}.{col_from_tmp}").format(
                col_to_set=final_col_ident, tmp_alias=md_alias, col_from_tmp=final_col_ident))

        update_sql = pgsql.SQL(
            "UPDATE {target_table} {tgt_alias} SET {set_clauses} FROM {temp_table} {tmp_alias} WHERE {tgt_alias}.hadm_id = {tmp_alias}.{hadm_col_in_temp};"
        ).format(
            target_table=target_table_ident, tgt_alias=target_alias,
            set_clauses=pgsql.SQL(', ').join(update_set_clauses),
            temp_table=temp_table_data_ident, tmp_alias=md_alias,
            hadm_col_in_temp=cte_hadm_id_for_grouping # hadm_id_cohort in temp table
        )
        drop_temp_table_sql = pgsql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=temp_table_data_ident)

        execution_steps = [(alter_sql, None), (create_temp_table_sql, params_for_cte), (update_sql, None), (drop_temp_table_sql, None)]
        return execution_steps, "execution_list", base_new_column_name, generated_column_details_for_preview
    else: # for_preview
        preview_select_cols = [
            pgsql.SQL("{}.subject_id").format(cohort_alias), 
            pgsql.SQL("{}.hadm_id").format(cohort_alias), 
            pgsql.SQL("{}.stay_id").format(cohort_alias) # Assuming cohort table has stay_id
        ]
        for _, final_col_ident, _, _ in selected_methods_details:
            preview_select_cols.append(pgsql.SQL("{md_alias}.{col_ident} AS {col_ident_preview}").format(
                md_alias=md_alias, col_ident=final_col_ident, col_ident_preview=final_col_ident
            ))

        preview_sql = pgsql.SQL(
            "WITH MergedDataCTE AS ({data_gen_query}) "
            "SELECT {select_cols_list} "
            "FROM {target_table} {coh_alias} " # Use cohort_alias here
            "LEFT JOIN MergedDataCTE {md_alias} ON {coh_alias}.hadm_id = {md_alias}.{hadm_col_in_temp} "
            "ORDER BY RANDOM() LIMIT {limit};"
        ).format(
            data_gen_query=data_generation_query_part,
            select_cols_list=pgsql.SQL(', ').join(preview_select_cols),
            target_table=target_table_ident, coh_alias=cohort_alias, # Pass cohort_alias
            md_alias=md_alias,
            hadm_col_in_temp=cte_hadm_id_for_grouping, # hadm_id_cohort in temp table
            limit=pgsql.Literal(preview_limit)
        )
        return preview_sql, None, params_for_cte, generated_column_details_for_preview

# --- END OF FILE sql_builder_special.py ---