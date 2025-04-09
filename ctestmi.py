let
    Source = SharePoint.Files("https://edisonintl.sharepoint.com/teams/GridOperationsAnalyticsandTech-TD/", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.StartsWith([Folder Path], "https://edisonintl.sharepoint.com/teams/GridOperationsAnalyticsandTech-TD/Projects/Validation CMI Performance")),
    #"Filtered Rows1" = Table.SelectRows(#"Filtered Rows", each [Name] = "validation_metrics_dataset.csv"),
    #"Filtered Hidden Files" = Table.SelectRows(#"Filtered Rows1", each [Attributes][Hidden]? <> true),
    
    // Invoke custom transform
    #"Invoke Custom Function" = Table.AddColumn(#"Filtered Hidden Files", "Transform File (4)", each #"Transform File (4)"([Content])),
    #"Renamed Columns2" = Table.RenameColumns(#"Invoke Custom Function", {"Name", "Source.Name"}),

    // Dynamically expand all columns in file
    #"Removed Other Columns2" = Table.SelectColumns(#"Renamed Columns2", {"Source.Name", "Transform File (4)"}),

    // THIS LINE was modified to dynamically load all columns!
    #"Expanded Table Column2" = Table.ExpandTableColumn(#"Removed Other Columns2", "Transform File (4)", Record.FieldNames(#"Transform File (4)"(#"Sample File (4)"))),

    // Promote headers and auto-type
    #"Promoted Headers" = Table.PromoteHeaders(#"Expanded Table Column2", [PromoteAllScalars=true]),
    #"Auto Typed" = Table.AutoDetectColumnTypes(#"Promoted Headers"),

    // Optional cleanup (leave these if needed, or remove safely)
    #"Replaced Value" = Table.ReplaceValue(#"Auto Typed", null, "N/A", Replacer.ReplaceValue, {"CMI_v", "SAIDI_v", "SAIFI_v", "MAIFI_v"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value", null, "N/A", Replacer.ReplaceValue, {"d_CMI", "d_SAIDI", "d_SAIFI", "d_MAIFI"}),

    // Capitalize region field (optional)
    #"Capitalized Each Word" = Table.TransformColumns(#"Replaced Value1", {{"REGION_NAME", Text.Proper, type text}})
in
    #"Capitalized Each Word"
