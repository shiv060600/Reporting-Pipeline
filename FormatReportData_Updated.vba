Sub FormatReportData()
    Dim wsData As Worksheet
    Dim wsReport As Worksheet
    Dim wsInputs As Worksheet
    Dim LastRow As Long
    Dim lastCol As Long
    Dim rng As Range
    Dim i As Long
    Dim selectedCustomer As String
    Dim selectedTitle As String
    Dim limit As Long

    ' Set reference to the INPUTS and REPORT_DATA worksheets
    On Error Resume Next
    Set wsInputs = ThisWorkbook.Sheets("INPUTS")
    Set wsData = ThisWorkbook.Sheets("REPORT_DATA")
    On Error GoTo 0

    If wsInputs Is Nothing Then
        MsgBox "INPUTS sheet not found!", vbExclamation
        Exit Sub
    End If
    If wsData Is Nothing Then
        MsgBox "REPORT_DATA sheet not found!", vbExclamation
        Exit Sub
    End If

    ' Get input values from named ranges (not used for filtering, just for reference)
    On Error Resume Next
    selectedCustomer = ThisWorkbook.Names("selected_dynamic_customer").RefersToRange.Value
    selectedTitle = ThisWorkbook.Names("selected_title").RefersToRange.Value
    limit = CLng(ThisWorkbook.Names("limit").RefersToRange.Value)
    On Error GoTo 0

    If selectedCustomer = "" Then selectedCustomer = ""
    If selectedTitle = "" Then selectedTitle = ""
    If limit < 0 Then limit = 0

    ' Find last row and column in REPORT_DATA
    LastRow = wsData.Cells(wsData.Rows.Count, 1).End(xlUp).Row
    lastCol = wsData.Cells(1, wsData.Columns.Count).End(xlToLeft).Column

    If LastRow < 2 Then
        MsgBox "No data found in REPORT_DATA beyond the header!", vbInformation
        Exit Sub
    End If

    ' CUSTOM SORTING LOGIC - Sort by WEBCAT2_DESCR with special category grouping
    Call SortDataByWebCat2(wsData, LastRow, lastCol)

    ' Use or create FINAL_REPORT sheet
    On Error Resume Next
    Set wsReport = ThisWorkbook.Sheets("FINAL_REPORT")
    If wsReport Is Nothing Then
        Set wsReport = ThisWorkbook.Sheets.Add(After:=wsData)
        wsReport.Name = "FINAL_REPORT"
    Else
        wsReport.Cells.Clear
    End If
    On Error GoTo 0

    ' Get header row
    Dim rawHeaders As Range
    Set rawHeaders = wsData.Range(wsData.Cells(1, 1), wsData.Cells(1, lastCol))

    ' Build dynamic list of headers to include
    Dim dynamicHeaders As Collection
    Set dynamicHeaders = New Collection

    Dim headerMap As Object
    Set headerMap = CreateObject("Scripting.Dictionary")

    Dim headerText As String
    Dim prettyLabel As String

    For i = 1 To rawHeaders.Columns.Count
        headerText = rawHeaders.Cells(1, i).Value

        Select Case True
            Case headerText = "TITLE"
                prettyLabel = "TITLE"
            Case headerText = "ISBN"
                prettyLabel = "ISBN"
            Case headerText = "NAMECUST"
                prettyLabel = "NAMECUST"
            Case headerText = "TUTTLE_SALES_CATEGORY"
                prettyLabel = "SALES_CATEGORY"
            Case headerText = "TYPE"
                prettyLabel = "TYPE"
            Case headerText = "PROD"
                prettyLabel = "PROD"
            Case headerText = "WEBCAT2"
                prettyLabel = "WEBCAT2"
            Case headerText = "WEBCAT2_DESCR"
                prettyLabel = "CATEGORY"
            Case headerText = "SUB"
                prettyLabel = "SUB"
            Case headerText = "RETAIL"
                prettyLabel = "RETAIL"
            Case headerText = "SEAS"
                prettyLabel = "SEAS"
            Case headerText = "ALL_ACCTS_12M_UNITS"
                prettyLabel = "ALL_ACCTS_12M_UNITS"
            Case headerText = "ALL_ACCTS_12M_DOLLARS"
                prettyLabel = "ALL_ACCTS_12M_$$"
            Case headerText = "12M_UNITS"
                prettyLabel = "12M Units"
            Case headerText = "12M_DOLLARS"
                prettyLabel = "12M $$"
            Case headerText = "YTD_UNITS"
                prettyLabel = "YTD Units"
            Case headerText = "YTD_DOLLARS"
                prettyLabel = "YTD $$"
            Case headerText Like "NET_UNITS_*_*"
                prettyLabel = "Units " & Replace(Mid(headerText, 11), "_", " ")
            Case headerText Like "UNITS_####"
                prettyLabel = "Units " & Mid(headerText, 7)
            Case Else
                prettyLabel = ""
        End Select

        If prettyLabel <> "" Then
            dynamicHeaders.Add Array(headerText, prettyLabel)
            headerMap(prettyLabel) = i
        End If
    Next i

    ' Write header and values to FINAL_REPORT
    wsReport.Cells(1, 1).Value = "NAME_PLACEHOLDER" & vbNewLine & _
                                 "Generated: " & Format(Now, "hh:mm AM/PM") & " on " & Format(Now, "dddd, mmmm d, yyyy")
    wsReport.Cells(1, 1).Font.Bold = True
    wsReport.Cells(1, 1).HorizontalAlignment = xlLeft
    wsReport.Rows(1).RowHeight = 30

    Dim colPair As Variant
    Dim headerCol As Long: headerCol = 2 ' start from column B

    For Each colPair In dynamicHeaders
        wsReport.Cells(1, headerCol).Value = colPair(1) ' pretty name
        wsReport.Cells(1, headerCol).Font.Bold = True

        Dim colIndex As Variant
        colIndex = Application.Match(colPair(0), rawHeaders, 0)
        If Not IsError(colIndex) Then
            wsData.Range(wsData.Cells(2, colIndex), wsData.Cells(LastRow, colIndex)).Copy
            wsReport.Cells(2, headerCol).PasteSpecial Paste:=xlPasteValues
        End If

        headerCol = headerCol + 1
    Next colPair

    ' Format the report
    With wsReport
        ' Auto-fit all columns that contain data (dynamic column range)
        .Range(.Cells(1, 1), .Cells(1, headerCol - 1)).EntireColumn.AutoFit
        
        ' Ensure minimum column width for readability and set maximum width to prevent excessive widths
        For i = 1 To headerCol - 1
            If .Columns(i).ColumnWidth < 8 Then
                .Columns(i).ColumnWidth = 8
            ElseIf .Columns(i).ColumnWidth > 25 Then
                .Columns(i).ColumnWidth = 25
            End If
        Next i

        LastRow = .Cells(.Rows.Count, 1).End(xlUp).Row
        Set rng = .Range(.Cells(2, 1), .Cells(LastRow, headerCol - 1))

        With rng.Borders
            .LineStyle = xlContinuous
            .Weight = xlThin
        End With

        ' Apply number formatting based on header labels
        For i = 2 To headerCol - 1
            Dim headerVal As String
            headerVal = wsReport.Cells(1, i).Value
        
            Select Case headerVal
                Case "RETAIL"
                    wsReport.Columns(i).NumberFormat = "$#,##0.00"
                Case "YTD $$", "12M $$", "ALL_ACCTS_12M_$$"
                    wsReport.Columns(i).NumberFormat = "$#,##0.00"
                Case Else
                    If headerVal Like "Units *" Or headerVal Like "*Units" Or headerVal = "ALL_ACCTS_12M_UNITS" Then
                        wsReport.Columns(i).NumberFormat = "#,##0"
                    End If
            End Select
        Next i

        ' Freeze top row
        .Activate
        .Cells(2, 1).Activate
        ActiveWindow.FreezePanes = True
    End With

    wsReport.Cells(1, 1).Select
    MsgBox "Report generated successfully in FINAL_REPORT", vbInformation
End Sub

Sub SortDataByWebCat2(wsData As Worksheet, LastRow As Long, lastCol As Long)
    Dim webCat2Col As Long
    Dim i As Long
    Dim headerText As String
    
    ' Find the WEBCAT2_DESCR column
    webCat2Col = 0
    
    For i = 1 To lastCol
        headerText = wsData.Cells(1, i).Value
        If headerText = "WEBCAT2_DESCR" Then
            webCat2Col = i
        End If
    Next i
    
    If webCat2Col = 0 Then
        MsgBox "WEBCAT2_DESCR column not found!", vbExclamation
        Exit Sub
    End If
    
    ' Add helper columns for sorting
    Dim categoryPrefixCol As Long
    Dim categorySuffixCol As Long
    
    categoryPrefixCol = lastCol + 1
    categorySuffixCol = lastCol + 2
    
    ' Add headers for helper columns
    wsData.Cells(1, categoryPrefixCol).Value = "TEMP_CATEGORY_PREFIX"
    wsData.Cells(1, categorySuffixCol).Value = "TEMP_CATEGORY_SUFFIX"
    
    ' Populate helper columns
    Dim cellValue As String
    Dim colonPos As Long
    
    For i = 2 To LastRow
        cellValue = Trim(wsData.Cells(i, webCat2Col).Value)
        
        If cellValue <> "" Then
            colonPos = InStr(cellValue, ":")
            
            If colonPos > 0 Then
                ' Split by colon
                wsData.Cells(i, categoryPrefixCol).Value = Trim(Left(cellValue, colonPos - 1))
                wsData.Cells(i, categorySuffixCol).Value = Trim(Mid(cellValue, colonPos + 1))
            Else
                ' No colon found, treat entire string as prefix
                wsData.Cells(i, categoryPrefixCol).Value = cellValue
                wsData.Cells(i, categorySuffixCol).Value = ""
            End If
        Else
            wsData.Cells(i, categoryPrefixCol).Value = ""
            wsData.Cells(i, categorySuffixCol).Value = ""
        End If
    Next i
    
    ' Sort the data with WEBCAT2_DESCR category grouping only
    Dim sortRange As Range
    Set sortRange = wsData.Range(wsData.Cells(1, 1), wsData.Cells(LastRow, categorySuffixCol))
    
    ' Clear any existing sort
    wsData.Sort.SortFields.Clear
    
    ' Add sort criteria in order of priority:
    ' 1. Category prefix (ascending) - groups categories together
    ' 2. Category suffix (ascending) - sorts within each category group
    
    wsData.Sort.SortFields.Add Key:=wsData.Cells(1, categoryPrefixCol), _
        SortOn:=xlSortOnValues, Order:=xlAscending, DataOption:=xlSortNormal
    
    wsData.Sort.SortFields.Add Key:=wsData.Cells(1, categorySuffixCol), _
        SortOn:=xlSortOnValues, Order:=xlAscending, DataOption:=xlSortNormal
    
    ' Apply the sort
    With wsData.Sort
        .SetRange sortRange
        .Header = xlYes
        .MatchCase = False
        .Orientation = xlTopToBottom
        .SortMethod = xlPinYin
        .Apply
    End With
    
    ' Clean up - remove helper columns
    wsData.Columns(categoryPrefixCol).Delete
    wsData.Columns(categorySuffixCol - 1).Delete  ' Adjust index after first deletion
    
End Sub
