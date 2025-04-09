let
    // STEP 1: Connect to SharePoint and find your file
    Source = SharePoint.Files("https://edisonintl.sharepoint.com/teams/GridOperationsAnalyticsandTech-TD/", [ApiVersion = 15]),
    FilteredFiles = Table.SelectRows(Source, each Text.StartsWith([Folder Path], "https://edisonintl.sharepoint.com/teams/GridOperationsAnalyticsandTech-TD/Projects/Validation CMI Performance") and [Name] = "validation_metrics_dataset.csv"),
    VisibleFile = Table.SelectRows(FilteredFiles, each [Attributes][Hidden]? <> true),

    // STEP 2: Grab the file content from the first row
    FileContent = VisibleFile{0}[Content],

    // STEP 3: Read the CSV and promote headers dynamically
    CSVTable = Csv.Document(FileContent, [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.None]),
    Promoted = Table.PromoteHeaders(CSVTable, [PromoteAllScalars = true]),

    // STEP 4: Automatically detect and apply types
    AutoTyped = Table.AutoDetectColumnTypes(Promoted)
in
    AutoTyped
