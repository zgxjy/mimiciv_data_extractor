**下一步行动计划（继续P0.3的测试和完善）：**

tcp://6.tcp.cpolar.cn:11914
5.  **测试SQL构建和执行的核心流程（预览和执行）：**
    *   对于**至少一个**配置完整的面板（**建议先重点测试 `CharteventsPanel`，因为它涉及到数值/文本提取的切换**）：
        *   \[ ] 点击“预览待合并数据”按钮。
            *   **观察和验证 `sql_builder_special.build_special_data_sql` (被 `_build_merge_query` 调用) 生成的预览SQL是否正确。**（可以在 `build_special_data_sql` 中打印最终的SQL和参数进行调试）。
            *   预览表格是否能正确显示数据？
        *   \[ ] 点击“执行合并到表”按钮。
            *   确认对话框是否正确显示了要添加/更新的列名和类型。
            *   **观察和验证 `build_special_data_sql` 生成的执行步骤（ALTER, CREATE TEMP, UPDATE, DROP）是否正确。**
            *   `MergeSQLWorker` 是否能成功执行这些步骤？
            *   数据库中的目标表是否按预期被修改？
            *   执行后的数据预览信号 (`request_preview_signal`) 是否能正确触发 `DataExportTab` 的预览？
