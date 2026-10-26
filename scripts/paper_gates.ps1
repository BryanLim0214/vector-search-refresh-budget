# Build the paper and print the gate checks (pages, errors, placeholders, style, citations, hand-typed numbers).
# Runs from the repository root whatever the current folder is. Python: $env:DVS_PYTHON if set, else
# .venv\Scripts\python.exe in the repository root. Needs TeX Live's latexmk on PATH (the MiKTeX latexmk
# without Perl fails silently). gen_numbers.py needs the raw run folders, which are not in this repository;
# without them it stops with an error and the build uses the committed paper\generated\numbers.tex.
$root = Split-Path -Parent $PSScriptRoot
Set-Location (Join-Path $root 'paper')
$py = if ($env:DVS_PYTHON) { $env:DVS_PYTHON } else { Join-Path $root '.venv\Scripts\python.exe' }
# One BLAS thread: under low commit memory, multi-threaded OpenBLAS fails to allocate.
$env:OPENBLAS_NUM_THREADS = '1'; $env:OMP_NUM_THREADS = '1'; $env:MKL_NUM_THREADS = '1'
& $py scripts\gen_numbers.py *> $env:TEMP\gen_numbers.log
"gen_numbers exit: $LASTEXITCODE"
latexmk -pdf -interaction=nonstopmode -quiet main.tex *> $null
$newest = (Get-ChildItem main.tex, sections\*.tex, generated\*.tex, figures\*.pdf, refs.bib | Measure-Object LastWriteTime -Maximum).Maximum
"build fresh: " + ((Get-Item main.pdf).LastWriteTime -ge $newest)
$log = Get-Content main.log -Raw
if ($log -match 'Output written on main.pdf \((\d+) pages') { "pages: $($Matches[1])" }
"errors: " + ([regex]::Matches($log, '(?m)^! ')).Count
"overfull: " + ([regex]::Matches($log, 'Overfull \\hbox')).Count
"undefined: " + ([regex]::Matches($log, 'undefined')).Count
$files = @('main.tex') + (Get-ChildItem sections\*.tex | ForEach-Object { $_.FullName })
$all = ($files | ForEach-Object { Get-Content $_ -Raw -Encoding UTF8 }) -join "`n"
"?? in numbers.tex: " + ([regex]::Matches((Get-Content generated\numbers.tex -Raw), '\?\?')).Count
"em dash: " + ([regex]::Matches($all, [char]0x2014)).Count + " / ---: " + ([regex]::Matches($all, '---')).Count
"intro cites: " + ([regex]::Matches((Get-Content sections\intro.tex -Raw), '\\cite')).Count
$bw = 'novel|robust|leverage|seamless|crucial|delve|notably|furthermore|moreover|state-of-the-art|paradigm|cutting-edge|comprehensive'
"banned: " + (([regex]::Matches($all, "\b($bw)\b", 'IgnoreCase') | ForEach-Object Value) -join ',')
# sources\refs.bib is the local citation ledger; it is not in this repository.
$ledger = Join-Path $root 'sources\refs.bib'
if (Test-Path $ledger) { "refs identical: " + ((Get-FileHash refs.bib).Hash -eq (Get-FileHash $ledger).Hash) } else { "refs identical: skipped (no sources\refs.bib)" }
# Double-blind: commit hashes, links, emails, author or institution names in the paper text or macros.
# Author and institution names are not stored here: set $env:DVS_BLIND_NAMES to a regex such as 'Name1|Name2'.
$names = if ($env:DVS_BLIND_NAMES) { '|' + $env:DVS_BLIND_NAMES } else { '' }
$blind = $all + "`n" + (Get-Content generated\numbers.tex -Raw)
"double-blind hits: " + (([regex]::Matches($blind, '(?i)\b[0-9a-f]{7,40}\b(?<!\b\d+\b)|github|gitlab|https?://|\S+@\S+\.\w+|COSC|universit' + $names) | ForEach-Object Value | Sort-Object -Unique) -join ',')
# Decimal numbers typed outside macros in results/conclusion/abstract (likely hand-typed results).
foreach ($f in 'sections\results.tex', 'sections\conclusion.tex', 'main.tex', 'sections\setup.tex', 'sections\intro.tex') {
    $i = 0
    foreach ($line in Get-Content $f -Encoding UTF8) {
        $i++
        if ($line -match '^\s*%') { continue }
        $hits = [regex]::Matches($line, '(?<![\\\w{])[+-]?\d+\.\d+') | ForEach-Object Value
        if ($hits) { "typed decimals ${f}:${i}: " + ($hits -join ' ') }
    }
}
