let
    // ❶ Point to the ROOT folder of your dataset
    Files = Folder.Files("C:\data\EventLogsLabeled_parquet"),

    // ❷ Keep only .parquet files (avoid csv/json etc. in same folder)
    OnlyParquet = Table.SelectRows(Files, each Text.Lower(Text.End([Name], 8)) = ".parquet"),

    // ❸ Read each file's binary using Parquet.Document
    WithTables = Table.AddColumn(OnlyParquet, "Data", each Parquet.Document([Content])),

    // ❹ Expand rows
    Expanded = Table.ExpandTableColumn(WithTables, "Data", Table.ColumnNames(WithTables{0}[Data])),

    // ❺ (Optional) If your partition key is only in the folder name like date_key=20250101,
    //     extract it; if the column already exists in the files, skip this step.
    AddDateKey =
        Table.AddColumn(
            Expanded, "date_key",
            each Number.FromText( Text.AfterDelimiter( Text.Select([Folder Path], {"0".."9","="}), "=" ) ),
            Int64.Type
        ),

    // ❻ (Optional) Set types for important columns
    Typed = Table.TransformColumnTypes(
        AddDateKey,
        {
            {"FOLLOWUP_DATETIME", type datetime},
            {"incident_id", Int64.Type},
            {"SYSTEM_OPID", type text},
            {"_phase", type text},
            {"date_key", Int64.Type}
        }
    )
in
    Typed
