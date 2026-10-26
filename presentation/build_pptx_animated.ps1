# Build the animated PowerPoint deck with PowerPoint itself, from the output of record_animations.py.
# Usage: powershell -File build_pptx_animated.ps1 <folder with manifest.json> <output .pptx>
# Each slide: a full-slide picture; looping videos for the animated regions (they start on their own
# and repeat until the next slide); build-in charts as pictures with a Float In entrance; a fade
# transition; the speaker script in the notes. PowerPoint runs without a window, and is closed again
# only if it was not already running.
param([string]$Build, [string]$Out)
$ErrorActionPreference = 'Stop'
$m = Get-Content (Join-Path $Build 'manifest.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$wasRunning = [bool](Get-Process POWERPNT -ErrorAction SilentlyContinue)
$app = New-Object -ComObject PowerPoint.Application
$pres = $app.Presentations.Add(0)                      # 0 = no window
try {
$pres.PageSetup.SlideWidth = 960; $pres.PageSetup.SlideHeight = 540
# Office document properties need late binding from PowerShell.
$props = $pres.BuiltInDocumentProperties
$titleProp = [System.__ComObject].InvokeMember('Item', [Reflection.BindingFlags]::GetProperty, $null, $props, @('Title'))
[void][System.__ComObject].InvokeMember('Value', [Reflection.BindingFlags]::SetProperty, $null, $titleProp, @($m.title))
$pres.SlideShowSettings.ShowMediaControls = 0
$k = 0.5                                               # slide pixels (1920 wide) to points (960 wide)
$i = 0
foreach ($s in $m.slides) {
    $i++
    $slide = $pres.Slides.Add($i, 12)                  # 12 = blank layout
    $bg = $slide.Shapes.AddPicture($s.bg, 0, -1, 0, 0, 960, 540)
    $bg.Name = 'Slide'; $bg.AlternativeText = $s.title
    foreach ($v in $s.videos) {
        $mv = $slide.Shapes.AddMediaObject2($v.mp4, 0, -1, $v.x * $k, $v.y * $k, $v.w * $k, $v.h * $k)
        $mv.Name = 'Animation ' + [IO.Path]::GetFileNameWithoutExtension($v.mp4)
        $mv.AlternativeText = 'Animation from the slide'
        $mv.MediaFormat.SetDisplayPictureFromFile($v.poster)
        $mv.MediaFormat.Muted = -1
        $ps = $mv.AnimationSettings.PlaySettings
        $ps.PlayOnEntry = -1; $ps.LoopUntilStopped = -1; $ps.HideWhileNotPlaying = 0; $ps.PauseAnimation = 0
        $ps.StopAfterSlides = 1
    }
    $n = 0
    foreach ($o in $s.overlays) {
        $pic = $slide.Shapes.AddPicture($o.png, 0, -1, $o.x * $k, $o.y * $k, $o.w * $k, $o.h * $k)
        $pic.Name = 'Chart ' + ($n + 1)
        $pic.AlternativeText = $s.title
        $trigger = if ($n -eq 0) { 3 } else { 2 }      # first after the transition, the rest together with it
        $eff = $slide.TimeLine.MainSequence.AddEffect($pic, 39, 0, $trigger)   # 39 = Ascend (Float In)
        $eff.Timing.Duration = 0.6
        $n++
    }
    # Every play effect starts on its own at the start of the slide, never on a click.
    foreach ($e in @($slide.TimeLine.MainSequence)) {
        if ($e.Timing.TriggerType -eq 1) { $e.Timing.TriggerType = 2 }
    }
    $slide.SlideShowTransition.EntryEffect = 3849      # fade smoothly
    $slide.SlideShowTransition.Duration = 0.5
    $slide.NotesPage.Shapes.Placeholders(2).TextFrame.TextRange.Text = ($s.notes -replace "`r?`n", "`r")
}
$pres.SaveAs($Out, 24)                                 # 24 = .pptx
} finally {
    $pres.Saved = -1
    $pres.Close()
    if (-not $wasRunning -and $app.Presentations.Count -eq 0) { $app.Quit() }
}
[void][Runtime.InteropServices.Marshal]::ReleaseComObject($app)
"wrote $Out"
