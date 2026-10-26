# Open the animated deck without a window and print what PowerPoint itself reports for each slide:
# videos (plays on its own, loops), entrance effects and their triggers, transition, notes length.
# Also exports the slides as pictures through PowerPoint, to compare with the source renders.
param([string]$Pptx, [string]$PngDir)
$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force $PngDir | Out-Null
$wasRunning = [bool](Get-Process POWERPNT -ErrorAction SilentlyContinue)
$app = New-Object -ComObject PowerPoint.Application
$pres = $app.Presentations.Open($Pptx, -1, 0, 0)      # read-only, no window
"slides: $($pres.Slides.Count); media controls shown: $($pres.SlideShowSettings.ShowMediaControls)"
foreach ($slide in $pres.Slides) {
    $vids = @($slide.Shapes | Where-Object { $_.Type -eq 16 })
    $vtxt = ($vids | ForEach-Object { $p = $_.AnimationSettings.PlaySettings; "$($_.Name)[auto=$($p.PlayOnEntry) loop=$($p.LoopUntilStopped)]" }) -join ' '
    $etxt = (@($slide.TimeLine.MainSequence) | ForEach-Object { "$($_.EffectType)/$($_.Timing.TriggerType)" }) -join ' '
    $notes = $slide.NotesPage.Shapes.Placeholders(2).TextFrame.TextRange.Text.Length
    "{0,2} shapes={1} videos: {2} | effects(type/trigger): {3} | transition={4} {5}s | notes={6} chars" -f `
        $slide.SlideIndex, $slide.Shapes.Count, $vtxt, $etxt, $slide.SlideShowTransition.EntryEffect, `
        $slide.SlideShowTransition.Duration, $notes
    $slide.Export((Join-Path $PngDir ("slide{0:D2}.png" -f $slide.SlideIndex)), 'PNG', 1920, 1080)
}
$pres.Close()
if (-not $wasRunning -and $app.Presentations.Count -eq 0) { $app.Quit() }
[void][Runtime.InteropServices.Marshal]::ReleaseComObject($app)
