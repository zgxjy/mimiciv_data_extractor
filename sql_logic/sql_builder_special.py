# --- START OF MODIFIED sql_builder_special.py ---
import psycopg2
import psycopg2.sql as pgsql # 使用别名 sql 可能会更简洁，但 pgsql 也很清晰
import time 
import traceback
from utils import validate_column_name
from app_config import SQL_AGGREGATES, AGGREGATE_RESULT_TYPES, DEFAULT_TEXT_VALUE_COLUMN, DEFAULT_VALUE_COLUMN

from typing import List, Tuple, Dict, Any, Optional

def build_special_data_sql(
    target_cohort_table_name: str,
    base_new_column_name: str,
    panel_specific_config: Dict[str, Any],
    for_execution: bool = False,
    preview_limit: int = 100
) -> Tuple[Optional[Any], Optional[str], Optional[List[Any]], List[Tuple[str, str]]]:
    generated_column_details_for_preview = [] 

    source_event_table = panel_specific_config.get("source_event_table")
    id_col_in_event_table = panel_specific_config.get("item_id_column_in_event_table")
    value_column_name_from_panel = panel_specific_config.get("value_column_to_extract") 
    time_col_for_window = panel_specific_config.get("time_column_in_event_table")
    selected_item_ids = panel_specific_config.get("selected_item_ids", [])
    aggregation_methods: Optional[Dict[str, bool]] = panel_specific_config.get("aggregation_methods")
    event_outputs: Optional[Dict[str, bool]] = panel_specific_config.get("event_outputs")
    current_time_window_text = panel_specific_config.get("time_window_text")
    cte_join_override = panel_specific_config.get("cte_join_on_cohort_override")

    if not all([source_event_table, id_col_in_event_table, current_time_window_text]):
        return None, "面板配置信息不完整 (源表,项目ID列,时间窗口)。", [], generated_column_details_for_preview
    if not selected_item_ids: 
        return None, "未选择任何要提取的项目ID。", [], generated_column_details_for_preview
    if not (aggregation_methods and any(aggregation_methods.values())) and \
       not (event_outputs and any(event_outputs.values())):
        return None, "未选择任何聚合方法或事件输出类型。", [], generated_column_details_for_preview

    try:
        schema_name, table_only_name = target_cohort_table_name.split('.')
    except ValueError:
        return None, f"目标队列表名 '{target_cohort_table_name}' 格式不正确 (应为 schema.table)。", [], []
        
    target_table_ident = pgsql.Identifier(schema_name, table_only_name)
    cohort_alias = pgsql.Identifier("cohort")
    event_alias = pgsql.Identifier("evt")
    event_admission_alias = pgsql.Identifier("adm_evt")
    md_alias = pgsql.Identifier("md")
    target_alias = pgsql.Identifier("target")

    params_for_cte = []
    item_id_filter_on_event_table_parts = []
    event_table_item_id_col_ident = pgsql.Identifier(id_col_in_event_table)

    if len(selected_item_ids) == 1:
        item_id_filter_on_event_table_parts.append(pgsql.SQL("{}.{} = %s").format(event_alias, event_table_item_id_col_ident))
        params_for_cte.append(selected_item_ids[0])
    elif len(selected_item_ids) > 1:
        item_id_filter_on_event_table_parts.append(pgsql.SQL("{}.{} IN %s").format(event_alias, event_table_item_id_col_ident))
        params_for_cte.append(tuple(selected_item_ids))
    else:
        return None, "内部错误: selected_item_ids 为空但通过了早期检查。", [], []

    time_filter_conditions_sql_parts = []
    cohort_icu_intime = pgsql.SQL("{}.icu_intime").format(cohort_alias)
    cohort_icu_outtime = pgsql.SQL("{}.icu_outtime").format(cohort_alias)
    cohort_admittime = pgsql.SQL("{}.admittime").format(cohort_alias)
    cohort_dischtime = pgsql.SQL("{}.dischtime").format(cohort_alias)
    actual_event_time_col_ident = pgsql.Identifier(time_col_for_window) if time_col_for_window else None

    from_join_clause_for_cte = pgsql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.hadm_id = {coh_alias}.hadm_id") \
                               .format(event_table=pgsql.SQL(source_event_table), evt_alias=event_alias,
                                       cohort_table=target_table_ident, coh_alias=cohort_alias)
    if source_event_table == "mimiciv_icu.chartevents": # Chartevents uses stay_id for ICU specific events
        from_join_clause_for_cte = pgsql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.stay_id = {coh_alias}.stay_id") \
                                   .format(event_table=pgsql.SQL(source_event_table), evt_alias=event_alias,
                                           cohort_table=target_table_ident, coh_alias=cohort_alias)
    if cte_join_override:
        try:
            from_join_clause_for_cte = cte_join_override.format(
                event_table=pgsql.SQL(source_event_table), evt_alias=event_alias,
                adm_evt=event_admission_alias,
                cohort_table=target_table_ident, coh_alias=cohort_alias
            )
        except KeyError as e:
             return None, f"JOIN覆盖SQL模板错误: 缺少占位符 {e}", params_for_cte, []

    is_value_source = bool(value_column_name_from_panel)
    is_text_extraction = (value_column_name_from_panel == DEFAULT_TEXT_VALUE_COLUMN)

    if is_value_source: 
        if not actual_event_time_col_ident: return None, f"值类型提取 ({source_event_table}) 需要时间列进行窗口化。", params_for_cte, []
        if current_time_window_text == "ICU入住后24小时": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '24 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
        elif current_time_window_text == "ICU入住后48小时": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '48 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
        elif current_time_window_text == "整个ICU期间": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime, end_ts=cohort_icu_outtime))
        elif current_time_window_text == "整个住院期间": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_admittime, end_ts=cohort_dischtime))
    else: 
        if current_time_window_text == "住院以前 (既往史)":
            if not cte_join_override: 
                return None, f"事件类型提取 ({source_event_table}) 选择“住院以前”时，必须提供JOIN覆盖逻辑。", params_for_cte, []
            time_filter_conditions_sql_parts.append(pgsql.SQL("{adm_evt}.admittime < {compare_ts}").format(adm_evt=event_admission_alias, compare_ts=cohort_admittime))
        elif actual_event_time_col_ident: 
            if current_time_window_text == "整个住院期间 (当前入院)":
                time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_admittime, end_ts=cohort_dischtime))
            elif current_time_window_text == "整个ICU期间 (当前入院)":
                time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime, end_ts=cohort_icu_outtime))

    select_event_cols_defs = [
        pgsql.SQL("{}.subject_id AS subject_id").format(cohort_alias),
        pgsql.SQL("{}.hadm_id AS hadm_id_cohort").format(cohort_alias)
    ]
    event_value_col_for_select_ident = pgsql.Identifier(value_column_name_from_panel) if value_column_name_from_panel else None

    if event_value_col_for_select_ident:
        select_event_cols_defs.append(pgsql.SQL("{}.{} AS event_value").format(event_alias, event_value_col_for_select_ident))
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

    selected_methods_details = []
    type_map_display = { "NUMERIC": "Numeric", "INTEGER": "Integer", "BOOLEAN": "Boolean", "TEXT": "Text", "DOUBLE PRECISION": "Numeric (Decimal)" }

    if aggregation_methods and any(aggregation_methods.values()):
        if not value_column_name_from_panel:
            return None, "值聚合方法被选择，但未指定要聚合的值列。", params_for_cte, []

        for method_key, is_selected in aggregation_methods.items():
            if is_selected:
                sql_template = SQL_AGGREGATES.get(method_key)
                if not sql_template:
                    print(f"警告: 未在 SQL_AGGREGATES 中找到方法 '{method_key}' 的模板。跳过此方法。")
                    continue
                col_type_str_raw = AGGREGATE_RESULT_TYPES.get(method_key, "NUMERIC")
                if is_text_extraction and method_key in ["MIN", "MAX", "FIRST_VALUE", "LAST_VALUE"]: # MIN/MAX 已在UI层面禁止了文本选择
                    col_type_str_raw = "TEXT"
                
                col_type_sql_obj = pgsql.SQL(col_type_str_raw)
                final_col_name_str = f"{base_new_column_name}_{method_key.lower()}"
                is_valid, err = validate_column_name(final_col_name_str)
                if not is_valid: return None, f"生成的列名 '{final_col_name_str}' 无效: {err}", params_for_cte, []
                selected_methods_details.append((final_col_name_str, pgsql.Identifier(final_col_name_str), sql_template, col_type_sql_obj))
                generated_column_details_for_preview.append((final_col_name_str, type_map_display.get(col_type_str_raw.upper(), col_type_str_raw)))

    elif event_outputs and any(event_outputs.values()):
        event_method_configs = {"exists": ("TRUE", pgsql.SQL("BOOLEAN")), "countevt": ("COUNT(*)", pgsql.SQL("INTEGER"))}
        for method_key, is_selected in event_outputs.items():
            if is_selected:
                if method_key not in event_method_configs: continue
                agg_template, col_type_sql_obj = event_method_configs[method_key]
                col_type_str_raw = str(col_type_sql_obj).replace('SQL(', '').replace(')', '').strip("'\"")
                final_col_name_str = f"{base_new_column_name}_{method_key.lower()}"
                is_valid, err = validate_column_name(final_col_name_str)
                if not is_valid: return None, f"生成的列名 '{final_col_name_str}' 无效: {err}", params_for_cte, []
                selected_methods_details.append((final_col_name_str, pgsql.Identifier(final_col_name_str), agg_template, col_type_sql_obj))
                generated_column_details_for_preview.append((final_col_name_str, type_map_display.get(col_type_str_raw.upper(), col_type_str_raw)))

    if not selected_methods_details:
        return None, "未能构建任何有效的提取列。", params_for_cte, generated_column_details_for_preview

    aggregated_columns_sql_list = []
    fe_val_ident_in_cte = pgsql.Identifier("event_value") # 来自 FilteredEvents CTE
    fe_time_ident_in_cte = pgsql.Identifier("event_time") # 来自 FilteredEvents CTE
    cte_hadm_id_for_grouping = pgsql.Identifier("hadm_id_cohort") # 或者 stay_id，取决于 join

    for _, final_col_ident, agg_sql_template_str, _ in selected_methods_details:
        params_for_template_format = {}
        
        # 对于 {val_col}: 使用 fe_val_ident_in_cte。JSONB_BUILD_OBJECT 会处理类型。
        if "{val_col}" in agg_sql_template_str:
            if not value_column_name_from_panel: # 早期的检查应该已经捕获了这个
                return None, f"聚合模板 '{agg_sql_template_str}' 需要值列，但未配置。", params_for_cte, []
            params_for_template_format['val_col'] = fe_val_ident_in_cte # 直接使用来自CTE的列
        
        # 对于 {time_col}: 使用 fe_time_ident_in_cte
        if "{time_col}" in agg_sql_template_str:
            if not time_col_for_window: # 早期的检查应该已经捕获了这个
                return None, f"聚合模板 '{agg_sql_template_str}' 需要时间列，但未配置。", params_for_cte, []
            params_for_template_format['time_col'] = fe_time_ident_in_cte

        # 构建 SQL 表达式
        if agg_sql_template_str.upper() == "COUNT(*)": # 特殊处理 COUNT(*)
            sql_expr_for_select = pgsql.SQL("COUNT(*)")
        else:
            # 确保所有模板中的占位符都有对应的参数
            # (这部分逻辑你已经有了，保持即可)
            template_placeholders = []
            if "{val_col}" in agg_sql_template_str: template_placeholders.append("val_col")
            if "{time_col}" in agg_sql_template_str: template_placeholders.append("time_col")
            
            missing_keys_in_params = [p for p in template_placeholders if p not in params_for_template_format]
            if missing_keys_in_params:
                 return None, f"聚合模板 '{agg_sql_template_str}' 的占位符 {missing_keys_in_params} 未在参数中提供。", params_for_cte, []

            try:
                sql_expr_for_select = pgsql.SQL(agg_sql_template_str).format(**params_for_template_format)
            except KeyError as e:
                 return None, f"格式化聚合模板 '{agg_sql_template_str}' 时出错: 占位符 {e} 未提供。", params_for_cte, []
        
        aggregated_columns_sql_list.append(pgsql.SQL("{} AS {}").format(sql_expr_for_select, final_col_ident))
    # --- END OF CRITICAL MODIFICATION ---

    group_by_clause_sql = pgsql.SQL("GROUP BY {}").format(cte_hadm_id_for_grouping)
    main_aggregation_select_sql = pgsql.SQL("SELECT {hadm_col}, {agg_cols} FROM FilteredEvents {group_by}").format(
        hadm_col=cte_hadm_id_for_grouping,
        agg_cols=pgsql.SQL(', ').join(aggregated_columns_sql_list),
        group_by=group_by_clause_sql
    )
    data_generation_query_part = pgsql.SQL("WITH {filtered_cte} {main_agg_select}").format(
        filtered_cte=filtered_events_cte_sql, main_agg_select=main_aggregation_select_sql)

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
            hadm_col_in_temp=cte_hadm_id_for_grouping
        )
        drop_temp_table_sql = pgsql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=temp_table_data_ident)

        execution_steps = [(alter_sql, None), (create_temp_table_sql, params_for_cte), (update_sql, None), (drop_temp_table_sql, None)]
        return execution_steps, "execution_list", base_new_column_name, generated_column_details_for_preview
    else: 
        preview_select_cols = [
            pgsql.SQL("{}.subject_id").format(cohort_alias), 
            pgsql.SQL("{}.hadm_id").format(cohort_alias), 
            pgsql.SQL("{}.stay_id").format(cohort_alias) 
        ]
        for _, final_col_ident, _, _ in selected_methods_details:
            preview_select_cols.append(pgsql.SQL("{md_alias}.{col_ident} AS {col_ident_preview}").format(
                md_alias=md_alias, col_ident=final_col_ident, col_ident_preview=final_col_ident
            ))

        preview_sql = pgsql.SQL(
            "WITH MergedDataCTE AS ({data_gen_query}) "
            "SELECT {select_cols_list} "
            "FROM {target_table} {coh_alias} "
            "LEFT JOIN MergedDataCTE {md_alias} ON {coh_alias}.hadm_id = {md_alias}.{hadm_col_in_temp} "
            "ORDER BY RANDOM() LIMIT {limit};"
        ).format(
            data_gen_query=data_generation_query_part,
            select_cols_list=pgsql.SQL(', ').join(preview_select_cols),
            target_table=target_table_ident, coh_alias=cohort_alias,
            md_alias=md_alias,
            hadm_col_in_temp=cte_hadm_id_for_grouping, 
            limit=pgsql.Literal(preview_limit)
        )
        return preview_sql, None, params_for_cte, generated_column_details_for_preview

# --- END OF MODIFIED sql_builder_special.py ---