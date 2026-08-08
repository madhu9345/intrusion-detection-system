$outputFile = "all_project_code.txt"

if (Test-Path $outputFile) {
    Remove-Item $outputFile
}

Get-ChildItem -Recurse -File |
Where-Object {
    $_.Extension -match '\.(java|js|ts|jsx|tsx|py|cpp|c|h|hpp|cs|html|css|scss|xml|json|yml|yaml|sql|properties|md)$'
} |
ForEach-Object {
    Add-Content $outputFile "`n===================================================="
    Add-Content $outputFile "File: $($_.FullName)"
    Add-Content $outputFile "===================================================="
    Get-Content $_.FullName | Add-Content $outputFile
}

Write-Host "Done! Output saved to $outputFile"