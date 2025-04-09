let
    Source = SharePoint.Files("https://edisonintl.sharepoint.com/teams/GridOperationsAnalyticsandTech-TD/", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.StartsWith([Folder Path], "https://edisonintl.sharepoint.com/teams/GridOperationsAnalyticsandTech-TD/Projects/Validation CMI Performance")),
    #"Filtered Rows1" = Table.SelectRows(#"Filtered Rows", each [Name] = "validation_metrics_dataset.csv"),
    #"Filtered Hidden Files" = Table.SelectRows(#"Filtered Rows1", each [Attributes][Hidden]? <> true),

    // 💡 Dynamically grab current column names from the file itself
    SampleFile = #"Filtered Hidden Files"{0}[Content],
    SampleHeaders = Table.ColumnNames(
        Table.PromoteHeaders(
            Csv.Document(SampleFile, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]),
            [PromoteAllScalars=true]
        )
    ),

    // Now safely invoke and expand the actual files
    #"Invoke Custom Function" = Table.AddColumn(#"Filtered Hidden Files", "Transform File (4)", each #"Transform File (4)"([Content])),
    #"Renamed Columns2" = Table.RenameColumns(#"Invoke Custom Function", {"Name", "Source.Name"}),
    #"Removed Other Columns2" = Table.SelectColumns(#"Renamed Columns2", {"Source.Name", "Transform File (4)"}),
    #"Expanded Table Column2" = Table.ExpandTableColumn(#"Removed Other Columns2", "Transform File (4)", SampleHeaders),

    // Continue with your existing cleanup
    #"Promoted Headers" = Table.PromoteHeaders(#"Expanded Table Column2", [PromoteAllScalars=true]),
    #"Auto Typed" = Table.AutoDetectColumnTypes(#"Promoted Headers")
in
    #"Auto Typed"
