# --- START OF FILE sql_builder_special.py ---
import psycopg2
import psycopg2.sql as pgsql
import time # For unique temp table names
import traceback
from utils import validate_column_name 

# 定义一些类型提示，如果需要的话
from typing import List, Tuple, Dict, Any, Optional

# 定义主构建函数
def build_special_data_sql(
    target_cohort_table_name: str,             
    base_new_column_name: str,                 
    panel_specific_config: Dict[str, Any],     
    for_execution: bool = False,
    preview_limit: int = 100
) -> Tuple[Optional[Any], Optional[str], Optional[List[Any]], List[Tuple[str, str]]]:
    """
    构建用于专项数据提取的SQL查询或执行步骤。
    
    # ... [参数和返回值文档保持不变] ...
    """
    generated_column_details_for_preview = [] # [(col_name_str, col_type_display_str), ...]
    
    # --- 1. 解析和验证 panel_specific_config ---
    source_event_table = panel_specific_config.get("source_event_table")
    id_col_in_event_table = panel_specific_config.get("item_id_column_in_event_table")
    value_column_to_extract = panel_specific_config.get("value_column_to_extract") # "valuenum", "value", or None
    time_col_for_window = panel_specific_config.get("time_column_in_event_table")
    selected_item_ids = panel_specific_config.get("selected_item_ids", [])
    
    aggregation_methods = panel_specific_config.get("aggregation_methods") # For value sources
    event_outputs = panel_specific_config.get("event_outputs")             # For event sources
    current_time_window_text = panel_specific_config.get("time_window_text")
    cte_join_override = panel_specific_config.get("cte_join_on_cohort_override")

    if not all([source_event_table, id_col_in_event_table, selected_item_ids, current_time_window_text]):
        return None, "面板配置信息不完整。", [], generated_column_details_for_preview
    if not (aggregation_methods or event_outputs):
        return None, "未选择任何聚合方法或事件输出类型。", [], generated_column_details_for_preview

    # --- 2. 定义SQL构建中常用的标识符 ---
    schema_name, table_only_name = target_cohort_table_name.split('.') # 假设总是 schema.table 格式
    target_table_ident = pgsql.Identifier(schema_name, table_only_name)
    cohort_alias = pgsql.Identifier("cohort")
    event_alias = pgsql.Identifier("evt")
    event_admission_alias = pgsql.Identifier("adm_evt")
    md_alias = pgsql.Identifier("md") 
    target_alias = pgsql.Identifier("target")

    # --- 3. 构建 FilteredEvents CTE 的参数和条件 ---
    params_for_cte = [] 
    item_id_filter_on_event_table_parts = []
    event_table_item_id_col_ident = pgsql.Identifier(id_col_in_event_table)
    if len(selected_item_ids) == 1:
        item_id_filter_on_event_table_parts.append(pgsql.SQL("{}.{} = %s").format(event_alias, event_table_item_id_col_ident))
        params_for_cte.append(selected_item_ids[0])
    else:
        item_id_filter_on_event_table_parts.append(pgsql.SQL("{}.{} IN %s").format(event_alias, event_table_item_id_col_ident))
        params_for_cte.append(tuple(selected_item_ids))
    
    time_filter_conditions_sql_parts = []
    cohort_icu_intime = pgsql.SQL("cohort.icu_intime") # 使用别名
    cohort_icu_outtime = pgsql.SQL("cohort.icu_outtime")
    cohort_admittime = pgsql.SQL("cohort.admittime")
    cohort_dischtime = pgsql.SQL("cohort.dischtime")
    actual_event_time_col_ident = pgsql.Identifier(time_col_for_window) if time_col_for_window else None
    
    # 默认的JOIN到队列表的方式
    from_join_clause_for_cte = pgsql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.hadm_id = {coh_alias}.hadm_id") \
                               .format(event_table=pgsql.SQL(source_event_table), evt_alias=event_alias, 
                                       cohort_table=target_table_ident, coh_alias=cohort_alias)

    if source_event_table == "mimiciv_icu.chartevents": # Chartevents 特殊处理 join key
        from_join_clause_for_cte = pgsql.SQL("FROM {event_table} {evt_alias} JOIN {cohort_table} {coh_alias} ON {evt_alias}.stay_id = {coh_alias}.stay_id") \
                                   .format(event_table=pgsql.SQL(source_event_table), evt_alias=event_alias, 
                                           cohort_table=target_table_ident, coh_alias=cohort_alias)
    
    # 应用面板可能提供的JOIN覆盖 (例如 "住院以前" 的情况)
    if cte_join_override:
        from_join_clause_for_cte = cte_join_override.format( # 确保占位符一致
            event_table=pgsql.SQL(source_event_table), evt_alias=event_alias,
            adm_evt=event_admission_alias, # 如果用到
            cohort_table=target_table_ident, coh_alias=cohort_alias
        )

    # 时间窗口条件构建
    is_value_source = bool(value_column_to_extract) # 根据是否有值列判断
    if is_value_source: # 如 labevents, chartevents
        if not actual_event_time_col_ident: return None, f"{source_event_table} 需要时间列进行窗口化提取。", params_for_cte, []
        if current_time_window_text == "ICU入住后24小时": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '24 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
        elif current_time_window_text == "ICU入住后48小时": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND ({start_ts} + interval '48 hours')").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime))
        elif current_time_window_text == "整个ICU期间": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime, end_ts=cohort_icu_outtime))
        elif current_time_window_text == "整个住院期间": time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_admittime, end_ts=cohort_dischtime))
    else: # 事件型数据源
        if current_time_window_text == "住院以前 (既往史)":
            # from_join_clause_for_cte 已经被 cte_join_override 处理了 (如果面板提供了)
            if not cte_join_override: # 如果面板没提供特定JOIN，但选了这个时间窗口，则可能出错或不准确
                # 对于 prescriptions，它只有 subject_id, hadm_id, 没有 admissions 的 admittime
                # 需要通过 hadm_id JOIN admissions 再 JOIN cohort on subject_id
                # 这个逻辑应该由面板的 get_panel_config() 返回的 cte_join_override 提供
                pass # 假设 cte_join_override 已正确设置
            time_filter_conditions_sql_parts.append(pgsql.SQL("{adm_evt}.admittime < {compare_ts}").format(adm_evt=event_admission_alias, compare_ts=cohort_admittime))

        elif current_time_window_text == "整个住院期间 (当前入院)" and actual_event_time_col_ident:
            time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_admittime, end_ts=cohort_dischtime))
        elif current_time_window_text == "整个ICU期间 (当前入院)" and actual_event_time_col_ident:
            time_filter_conditions_sql_parts.append(pgsql.SQL("{evt}.{time_col} BETWEEN {start_ts} AND {end_ts}").format(evt=event_alias, time_col=actual_event_time_col_ident, start_ts=cohort_icu_intime, end_ts=cohort_icu_outtime))
            
    # --- 4. 构建 FilteredEvents CTE ---
    select_event_cols_defs = [
        pgsql.SQL("cohort.subject_id AS subject_id"), # 从 cohort 获取
        pgsql.SQL("cohort.hadm_id AS hadm_id_cohort") # 从 cohort 获取
    ]
    if value_column_to_extract:
        select_event_cols_defs.append(pgsql.SQL("evt.{} AS event_value").format(pgsql.Identifier(value_column_to_extract)))
    if time_col_for_window: 
        select_event_cols_defs.append(pgsql.SQL("evt.{} AS event_time").format(pgsql.Identifier(time_col_for_window)))

    all_where_conditions_sql_parts = item_id_filter_on_event_table_parts + time_filter_conditions_sql_parts
    filtered_events_cte_sql = pgsql.SQL(
        "FilteredEvents AS (SELECT DISTINCT {select_list} {from_join_clause} WHERE {conditions})"
    ).format(
        select_list=pgsql.SQL(', ').join(select_event_cols_defs),
        from_join_clause=from_join_clause_for_cte,
        conditions=pgsql.SQL(' AND ').join(all_where_conditions_sql_parts) if all_where_conditions_sql_parts else pgsql.SQL("TRUE")
    )

    # --- 5. 构建聚合逻辑 ---
    selected_methods_details = [] 
    type_map = { "NUMERIC": "Numeric", "INTEGER": "Integer", "BOOLEAN": "Boolean", "TEXT": "Text" }
    is_text_extraction = (value_column_to_extract == "value")
    
    # 新方式：不需要数据库连接获取 SQL 类型字符串
    def get_sql_type_str(sql_obj):
        """从 SQL 对象获取类型字符串"""
        if isinstance(sql_obj, pgsql.SQL):
            # 提取字符串内容并清理
            raw_str = str(sql_obj)
            # 移除 SQL(...) 和引号
            clean_str = raw_str.replace('SQL(', '').replace(')', '').strip("'\"")
            return clean_str
        return "UNKNOWN"
    
    if aggregation_methods: # 处理数值或文本值的聚合
        method_configs = []
        if not is_text_extraction: # 数值型
             method_configs = [
                ("first", "(array_agg({val} ORDER BY {time} ASC NULLS LAST))[1]", pgsql.SQL("NUMERIC")),
                ("last", "(array_agg({val} ORDER BY {time} DESC NULLS LAST))[1]", pgsql.SQL("NUMERIC")),
                ("min", "MIN({val})", pgsql.SQL("NUMERIC")), ("max", "MAX({val})", pgsql.SQL("NUMERIC")),
                ("mean", "AVG({val})", pgsql.SQL("NUMERIC")), ("countval", "COUNT({val})", pgsql.SQL("INTEGER")),
            ]
        else: # 文本型
            method_configs = [
                ("first", "(array_agg({val} ORDER BY {time} ASC NULLS LAST))[1]", pgsql.SQL("TEXT")), # suffix: _first_text
                ("last", "(array_agg({val} ORDER BY {time} DESC NULLS LAST))[1]", pgsql.SQL("TEXT")),  # _last_text
                ("min", "MIN(CAST({val} AS TEXT))", pgsql.SQL("TEXT")), # _min_text
                ("max", "MAX(CAST({val} AS TEXT))", pgsql.SQL("TEXT")), # _max_text
                ("countval", "COUNT(CASE WHEN {val} IS NOT NULL AND CAST({val} AS TEXT) <> '' THEN 1 END)", pgsql.SQL("INTEGER")) # _counttext
            ]

        for method_key, agg_template, col_type_sql_obj in method_configs:
            if aggregation_methods.get(method_key): # 如果面板中这个方法被选中了
                actual_suffix = method_key
                if is_text_extraction: # 为文本添加后缀以区分
                    if method_key not in ["countval"]: actual_suffix = f"{method_key}_text"
                    else: actual_suffix = "counttext"
                
                final_col_name_str = f"{base_new_column_name}_{actual_suffix}"
                is_valid, err = validate_column_name(final_col_name_str)
                if not is_valid: return None, f"列名错误 ({final_col_name_str}): {err}", params_for_cte, []
                
                selected_methods_details.append((final_col_name_str, pgsql.Identifier(final_col_name_str), agg_template, col_type_sql_obj))
                
                # 无需数据库连接的 SQL 类型字符串
                col_type_str_raw = get_sql_type_str(col_type_sql_obj)
                generated_column_details_for_preview.append((final_col_name_str, type_map.get(col_type_str_raw.upper(), col_type_str_raw)))

    elif event_outputs: # 事件型输出
        method_configs = [("exists", "TRUE", pgsql.SQL("BOOLEAN")), ("countevt", "COUNT(*)", pgsql.SQL("INTEGER"))]
        for method_key, agg_template, col_type_sql_obj in method_configs:
            if event_outputs.get(method_key):
                final_col_name_str = f"{base_new_column_name}_{method_key}"
                is_valid, err = validate_column_name(final_col_name_str)  # 使用正确的函数
                if not is_valid: return None, f"列名错误 ({final_col_name_str}): {err}", params_for_cte, []
                selected_methods_details.append((final_col_name_str, pgsql.Identifier(final_col_name_str), agg_template, col_type_sql_obj))
                
                # 无需数据库连接的 SQL 类型字符串
                col_type_str_raw = get_sql_type_str(col_type_sql_obj)
                generated_column_details_for_preview.append((final_col_name_str, type_map.get(col_type_str_raw.upper(), col_type_str_raw)))


    if not selected_methods_details:
        return None, "未选择任何有效的提取方式或输出类型。", params_for_cte, generated_column_details_for_preview
    
    aggregated_columns_sql_list = []
    fe_val_ident = pgsql.Identifier("event_value") 
    fe_time_ident = pgsql.Identifier("event_time")
    cte_hadm_id_for_grouping = pgsql.Identifier("hadm_id_cohort") 
    group_by_hadm_id_sql = pgsql.SQL(" GROUP BY {}").format(cte_hadm_id_for_grouping)

    for _, final_col_ident, agg_template_str, _ in selected_methods_details:
        sql_expr_str = agg_template_str
        sql_expr = None
        if "{val}" in sql_expr_str and "{time}" in sql_expr_str:
            if not value_column_to_extract or not time_col_for_window: return None, f"提取方式 '{agg_template_str}' 需要值列和时间列。", params_for_cte, []
            sql_expr = pgsql.SQL(sql_expr_str).format(val=fe_val_ident, time=fe_time_ident)
        elif "{val}" in sql_expr_str:
            if not value_column_to_extract: return None, f"提取方式 '{agg_template_str}' 需要值列。", params_for_cte, []
            sql_expr = pgsql.SQL(sql_expr_str).format(val=fe_val_ident)
        else: sql_expr = pgsql.SQL(sql_expr_str)
        aggregated_columns_sql_list.append(pgsql.SQL("{} AS {}").format(sql_expr, final_col_ident))
    
    main_aggregation_select_sql = pgsql.SQL("SELECT {hadm_grp_col}, {agg_cols} FROM FilteredEvents {gb}").format(
        hadm_grp_col=cte_hadm_id_for_grouping,
        agg_cols=pgsql.SQL(', ').join(aggregated_columns_sql_list),
        gb=group_by_hadm_id_sql
    )
    data_generation_query_part = pgsql.SQL("WITH {filtered_cte} {main_agg_select}").format(
        filtered_cte=filtered_events_cte_sql, main_agg_select=main_aggregation_select_sql)

    # --- 6. 根据 for_execution 返回执行步骤或预览SQL ---
    if for_execution:
        alter_clauses = []
        for _, final_col_ident, _, col_type_sql_obj in selected_methods_details:
            alter_clauses.append(pgsql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(final_col_ident, col_type_sql_obj))
        alter_sql = pgsql.SQL("ALTER TABLE {target_table} ").format(target_table=target_table_ident) + pgsql.SQL(', ').join(alter_clauses) + pgsql.SQL(";")
        
        temp_table_data_name_str = f"temp_merge_data_{base_new_column_name.lower()}_{int(time.time()) % 100000}"
        if len(temp_table_data_name_str) > 63: temp_table_data_name_str = temp_table_data_name_str[:63]
        temp_table_data_ident = pgsql.Identifier(temp_table_data_name_str)
        create_temp_table_sql = pgsql.SQL("CREATE TEMPORARY TABLE {temp_table} AS ({data_gen_query});").format(temp_table=temp_table_data_ident, data_gen_query=data_generation_query_part)
        
        update_set_clauses = []
        for _, final_col_ident, _, _ in selected_methods_details:
            update_set_clauses.append(pgsql.SQL("{col_to_set} = {tmp_alias}.{col_from_tmp}").format(col_to_set=final_col_ident, tmp_alias=md_alias, col_from_tmp=final_col_ident))
        
        update_sql = pgsql.SQL("UPDATE {target_table} {tgt_alias} SET {set_clauses} FROM {temp_table} {tmp_alias} WHERE {tgt_alias}.hadm_id = {tmp_alias}.{hadm_col_in_temp};").format(
            target_table=target_table_ident, tgt_alias=target_alias, 
            set_clauses=pgsql.SQL(', ').join(update_set_clauses), 
            temp_table=temp_table_data_ident, tmp_alias=md_alias, 
            hadm_col_in_temp=cte_hadm_id_for_grouping 
        )
        drop_temp_table_sql = pgsql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=temp_table_data_ident)
        
        execution_steps = [(alter_sql, None), (create_temp_table_sql, params_for_cte), (update_sql, None), (drop_temp_table_sql, None)]
        return execution_steps, "execution_list", base_new_column_name, generated_column_details_for_preview
    else: # for_preview
        preview_select_cols = [
            pgsql.SQL("cohort.subject_id"), pgsql.SQL("cohort.hadm_id"), pgsql.SQL("cohort.stay_id")
        ]
        for _, final_col_ident, _, _ in selected_methods_details: 
            preview_select_cols.append(pgsql.SQL("{md_alias}.{col_ident} AS {col_ident_preview}").format(
                md_alias=md_alias, col_ident=final_col_ident, col_ident_preview=final_col_ident
            ))

        preview_sql = pgsql.SQL(
            "WITH MergedDataCTE AS ({data_gen_query}) "
            "SELECT {select_cols_list} "
            "FROM {target_table} cohort " # explicit alias for cohort
            "LEFT JOIN MergedDataCTE {md_alias} ON cohort.hadm_id = {md_alias}.{hadm_col_in_temp} "
            "ORDER BY RANDOM() LIMIT {limit};"
        ).format(
            data_gen_query=data_generation_query_part,
            select_cols_list=pgsql.SQL(', ').join(preview_select_cols),
            target_table=target_table_ident, # cohort_alias is now 'cohort'
            md_alias=md_alias,
            hadm_col_in_temp=cte_hadm_id_for_grouping,
            limit=pgsql.Literal(preview_limit)
        )
        return preview_sql, None, params_for_cte, generated_column_details_for_preview

# --- END OF FILE sql_builder_special.py ---