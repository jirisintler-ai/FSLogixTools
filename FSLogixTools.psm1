# FSLogixTools.psm1
# Version 0.9.5
# PowerShell 5.1 compatible

#region Helpers

function Get-FTModuleRoot {
    $script:FT_ModuleRoot = $script:FT_ModuleRoot -as [string]
    if (-not $script:FT_ModuleRoot) {
        if ($PSCommandPath) {
            $script:FT_ModuleRoot = Split-Path -Path $PSCommandPath -Parent
        } else {
            $script:FT_ModuleRoot = Split-Path -Path $MyInvocation.MyCommand.Path -Parent
        }
    }
    return $script:FT_ModuleRoot
}

function Get-FTText {
    [CmdletBinding()]
    param(
        [Parameter(Position=0)][object]$Config,
        [Parameter(Position=1,Mandatory=$true)][string]$Key,
        [Parameter(Position=2)][string]$Default
    )
    try {
        if ($Config -and $Config.PSObject.Properties['Gui'] -and $Config.Gui -and $Config.Gui.PSObject.Properties['Texts'] -and $Config.Gui.Texts) {
            $v = $Config.Gui.Texts.$Key
            if ($null -ne $v -and -not [string]::IsNullOrWhiteSpace("" + $v)) { return ("" + $v) }
        }
    } catch { }
    if ($null -ne $Default) { return $Default } else { return $Key }
}

function Get-FTTip {
    [CmdletBinding()]
    param(
        [Parameter(Position=0)][object]$Config,
        [Parameter(Position=1,Mandatory=$true)][string]$Key,
        [Parameter(Position=2)][string]$Default
    )
    try {
        if ($Config -and $Config.PSObject.Properties['Gui'] -and $Config.Gui -and $Config.Gui.PSObject.Properties['Tooltips'] -and $Config.Gui.Tooltips) {
            $v = $Config.Gui.Tooltips.$Key
            if ($null -ne $v -and -not [string]::IsNullOrWhiteSpace("" + $v)) { return ("" + $v) }
        }
    } catch { }
    if ($null -ne $Default) { return $Default } else { return $Key }
}

function Normalize-FTConfig {
    [CmdletBinding()]
    param([Parameter(Mandatory=$true)][object]$Config)
    try {
        if (-not ($Config.PSObject.Properties['Gui'] -and $Config.Gui)) { Add-Member -InputObject $Config -NotePropertyName Gui -NotePropertyValue ([pscustomobject]@{}) -Force }
        if (-not ($Config.Gui.PSObject.Properties['Texts'] -and $Config.Gui.Texts)) { Add-Member -InputObject $Config.Gui -NotePropertyName Texts -NotePropertyValue ([pscustomobject]@{}) -Force }
        if (-not ($Config.Gui.PSObject.Properties['Tooltips'] -and $Config.Gui.Tooltips)) { Add-Member -InputObject $Config.Gui -NotePropertyName Tooltips -NotePropertyValue ([pscustomobject]@{}) -Force }

        $t = $Config.Gui.Texts
        $defaults = @{
            AppTitle                 = 'FSLogixTools'
            NewRequestTitle          = 'New Request'
            RequestTypeLabel         = 'Request Type*'
            TemplateLabel            = 'Template*'
            WorkersLabel             = 'Worker(s)'
            AllWorkersCheck          = 'All'
            ParametersLabel          = 'Parameters'
            ScheduledLabel           = 'Scheduled (UTC)'
            PriorityLabel            = 'Priority'
            NotesLabel               = 'Notes'
            CreateButton             = 'Create'
            CancelButton             = 'Cancel'
            NewRequestCreatedInfo    = 'Request(s) created: {0}'
            CreateFailedTitle        = 'FSLogixTools - Error'
            CreateFailedMessage      = 'Create failed: {0}'
            TemplateLoadFailed       = 'Template load failed: {0}'
            NoParametersDefined      = 'No parameters defined.'
            StateLabel               = 'State:'
            WorkerLabel              = 'Worker:'
            AutoLabel                = 'Auto:'
            RefreshButton            = 'Refresh'
            NewRequestButton         = 'New Request'
            QueueLabel               = 'Queued Requests (incl. processed)'
            TotalStatus              = 'Total: {0}'
            ShownStatus              = 'Shown: {0}'
            InProgressStatus         = 'InProgress: {0}'
            ConfigNotFoundMsg        = 'Configuration file not found or failed to load.'
            InvalidConfigMsg         = 'Invalid configuration. Missing: {0}'
            FailedLoadQueueMsg       = 'Failed to load queue: {0}'
            PopulateWorkersFailedMsg = 'Failed to populate workers: {0}'
        }
        foreach ($k in $defaults.Keys) { if (-not $t.PSObject.Properties[$k]) { Add-Member -InputObject $t -NotePropertyName $k -NotePropertyValue $defaults[$k] -Force } }

        $tp = $Config.Gui.Tooltips
        $tpDefaults = @{
            RefreshButton  = 'Reload the queue view'
            NewRequestButton= 'Create a new request'
            StateFilter    = 'Filter the queue by state'
            WorkerFilter   = 'Filter the queue by worker'
            AutoRefresh    = 'Select auto refresh interval'
            Grid           = 'Queue overview'
            TemplateCombo  = 'Pick a template file'
            WorkersList    = 'Select target workers'
            AllWorkersCheck= 'Toggle all workers on/off'
            DateTime       = 'Optional UTC schedule'
            Priority       = 'Request priority'
            Notes          = 'Additional notes'
            CreateButton   = 'Create the request(s)'
            CancelButton   = 'Close dialog without creating'
            ParametersPanel= 'Template-defined parameters'
        }
        foreach ($k in $tpDefaults.Keys) { if (-not $tp.PSObject.Properties[$k]) { Add-Member -InputObject $tp -NotePropertyName $k -NotePropertyValue $tpDefaults[$k] -Force } }
    } catch { }
    return $Config
}

function Validate-FTConfig {
    [CmdletBinding()]
    param([Parameter(Mandatory=$true)][object]$Config)
    # Basic type checks (non-intrusive)
    $errors = @()
    try {
        if ($Config.PSObject.Properties['Paths'] -and $Config.Paths) {
            if ($Config.Paths.PSObject.Properties['RootPath'] -and ($Config.Paths.RootPath) -and -not ($Config.Paths.RootPath -is [string])) { $errors += 'Paths.RootPath must be string' }
        }
        if ($Config.PSObject.Properties['Gui'] -and $Config.Gui) {
            if ($Config.Gui.PSObject.Properties['AutoRefreshSeconds'] -and ($Config.Gui.AutoRefreshSeconds) -and -not ($Config.Gui.AutoRefreshSeconds -is [int])) { $errors += 'Gui.AutoRefreshSeconds must be int' }
            if ($Config.Gui.PSObject.Properties['AutoOptions'] -and ($Config.Gui.AutoOptions) -and -not ($Config.Gui.AutoOptions -is [System.Collections.IEnumerable])) { $errors += 'Gui.AutoOptions must be array' }
            if ($Config.Gui.PSObject.Properties['AutoRefreshOptions'] -and ($Config.Gui.AutoRefreshOptions) -and -not ($Config.Gui.AutoRefreshOptions -is [System.Collections.IEnumerable])) { $errors += 'Gui.AutoRefreshOptions must be array' }
            if ($Config.Gui.PSObject.Properties['Priorities'] -and ($Config.Gui.Priorities) -and -not ($Config.Gui.Priorities -is [System.Collections.IEnumerable])) { $errors += 'Gui.Priorities must be array' }
        }
        if ($Config.PSObject.Properties['Agent'] -and $Config.Agent) {
            if ($Config.Agent.PSObject.Properties['MaxParallel'] -and ($Config.Agent.MaxParallel) -and -not ($Config.Agent.MaxParallel -is [int])) { $errors += 'Agent.MaxParallel must be int' }
            if ($Config.Agent.PSObject.Properties['MaxRequestsPerCycle'] -and ($Config.Agent.MaxRequestsPerCycle) -and -not ($Config.Agent.MaxRequestsPerCycle -is [int])) { $errors += 'Agent.MaxRequestsPerCycle must be int' }
        }
    } catch { $errors += ('Validation exception: ' + $_.Exception.Message) }
    return $errors
}
function Get-FTConfig {
    [CmdletBinding()]
    param(
        [Parameter(ValueFromPipelineByPropertyName=$true, Position=0)]
        [Alias('Config','Path')]
        [string] $ConfigPath
    )

    $candidates = New-Object System.Collections.ArrayList
    if ($ConfigPath) { [void]$candidates.Add($ConfigPath) }
    $moduleRoot = Get-FTModuleRoot
    if ($moduleRoot) { [void]$candidates.Add((Join-Path $moduleRoot 'FSLogixTools.json')) }
    [void]$candidates.Add((Join-Path (Get-Location).Path 'FSLogixTools.json'))

    foreach ($p in $candidates) {
        if (Test-Path -LiteralPath $p) {
            try {
                $json = Get-Content -LiteralPath $p -Raw -Encoding UTF8 -ErrorAction Stop
                $cfg  = $json | ConvertFrom-Json -ErrorAction Stop
                Add-Member -InputObject $cfg -NotePropertyName SourcePath -NotePropertyValue $p -Force
                return $cfg
            } catch {
                throw "Failed to read/parse configuration file '$p': $($_.Exception.Message)"
            }
        }
    }

    # Build message listing candidate paths without relying on Select-Object -Unique (avoids IComparable on mixed types)
    $seen = @{}
    $uniq = @()
    foreach ($it in $candidates) { $s = "" + $it; if (-not $seen.ContainsKey($s)) { $seen[$s] = $true; $uniq += $s } }
    $list = ($uniq) -join "`r`n  - "
    throw ("Configuration file not found. Looked in:`r`n  - {0}" -f $list)
}

function Get-FTPaths {
    [CmdletBinding()]
    param(
        [Parameter(ValueFromPipeline=$true)][object] $Config
    )
    $cfg = $Config
    if (-not $cfg) { try { $cfg = Get-FTConfig } catch { $cfg = $null } }
    $root = $null
    try {
        if ($cfg -and $cfg.PSObject.Properties['Paths'] -and $cfg.Paths -and $cfg.Paths.PSObject.Properties['RootPath'] -and $cfg.Paths.RootPath) {
            $root = "" + $cfg.Paths.RootPath
        }
    } catch { $root = $null }
    $requests = $null; $processed = $null; $logs = $null; $templates = $null
    if ($root) {
        try { $requests  = Join-Path $root 'Requests' } catch { }
        try { $processed = Join-Path $root 'Processed' } catch { }
        try { $logs      = Join-Path $root 'Logs' } catch { }
        try { $templates = Join-Path $root 'Templates' } catch { }
    }
    # Backward compatibility: if explicit paths exist, prefer them
    try { if ($cfg -and $cfg.Paths.PSObject.Properties['Requests']  -and $cfg.Paths.Requests)  { $requests  = "" + $cfg.Paths.Requests } } catch { }
    try { if ($cfg -and $cfg.Paths.PSObject.Properties['Processed'] -and $cfg.Paths.Processed) { $processed = "" + $cfg.Paths.Processed } } catch { }
    try { if ($cfg -and $cfg.Paths.PSObject.Properties['Logs']      -and $cfg.Paths.Logs)      { $logs      = "" + $cfg.Paths.Logs } } catch { }
    try { if ($cfg -and $cfg.Paths.PSObject.Properties['Templates'] -and $cfg.Paths.Templates) { $templates = "" + $cfg.Paths.Templates } } catch { }

    return [pscustomobject]@{
        Root      = $root
        Requests  = $requests
        Processed = $processed
        Logs      = $logs
        Templates = $templates
    }
}

function Ensure-FTDirectories {
    [CmdletBinding()]
    param([object]$Config)
    try {
        $p = Get-FTPaths -Config $Config
        foreach ($d in @($p.Requests,$p.Processed,$p.Logs,$p.Templates)) {
            if ($d) { New-FTDirectory -Path $d }
        }
    } catch { }
}

function New-FTDirectory {
    param([Parameter(Mandatory=$true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}

function Write-FTLog {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)][string] $Message,
        [ValidateSet('Trace','Info','Warn','Error')][string] $Level = 'Info',
        [object] $Config
    )
    # Try to get config, but don't fail log if config cannot be loaded
    $cfg = $null
    try {
        if ($Config) { $cfg = $Config } else { $cfg = Get-FTConfig }
    } catch { $cfg = $null }

    # honor log level filtering when a config is provided
    $shouldWrite = $true
    try {
        # define severity order from least to most severe
        $levelMap = @{ 'Trace' = 0; 'Info' = 1; 'Warn' = 2; 'Error' = 3 }
        if ($cfg -and $cfg.PSObject.Properties['Agent'] -and $cfg.Agent -and $cfg.Agent.PSObject.Properties['LogLevel']) {
            $minName = "" + $cfg.Agent.LogLevel
            if ($levelMap.ContainsKey($minName) -and $levelMap.ContainsKey($Level)) {
                $minVal = $levelMap[$minName]
                $curVal = $levelMap[$Level]
                if ($curVal -lt $minVal) { $shouldWrite = $false }
            }
        }
    } catch { $shouldWrite = $true }
    if (-not $shouldWrite) { return }

    # Resolve log directory (from RootPath or explicit Logs); fallback to ProgramData
    $logsRoot = $null
    try {
        $paths = Get-FTPaths -Config $cfg
        if ($paths -and $paths.Logs) { $logsRoot = "" + $paths.Logs }
    } catch { $logsRoot = $null }
    if (-not $logsRoot -or [string]::IsNullOrWhiteSpace($logsRoot)) {
        $logsRoot = Join-Path $env:ProgramData 'FSLogixTools\Logs'
    }

    $date = Get-Date
    $file = Join-Path $logsRoot ("{0}_{1:yyyyMMdd}.log" -f $env:COMPUTERNAME, $date)
    $line = "[{0:yyyy-MM-dd HH:mm:ss.fff}] [{1}] {2}" -f $date, $Level.ToUpper(), $Message

    $enc = New-Object System.Text.UTF8Encoding($true)  # UTF-8 BOM
    try {
        New-FTDirectory -Path $logsRoot
        [System.IO.File]::AppendAllText($file, $line + [Environment]::NewLine, $enc)
    } catch {
        # final fallback: ProgramData
        try {
            $fallback = Join-Path $env:ProgramData 'FSLogixTools\Logs'
            New-FTDirectory -Path $fallback
            $file2 = Join-Path $fallback ("{0}_{1:yyyyMMdd}.log" -f $env:COMPUTERNAME, $date)
            [System.IO.File]::AppendAllText($file2, $line + [Environment]::NewLine, $enc)
        } catch { }
    }
}

function Write-FTState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)][string] $Path,
        [Parameter(Mandatory=$true)][ValidateSet('New','Queued','InProgress','Completed','Failed')][string] $State,
        [string] $Type = 'Request',
        [int] $Percent = 0,
        [string] $Message,
        [string] $Worker,
        [string] $RequestId,
        [string] $CompletedUtc
    )
    if (($State -eq 'Completed' -or $State -eq 'Failed') -and -not $CompletedUtc) {
        $CompletedUtc = [DateTime]::UtcNow.ToString('o')
    }
    $obj = [pscustomobject]@{
        Id          = $RequestId
        Type        = $Type
        State       = $State
        Percent     = $Percent
        Message     = $Message
        Worker      = $Worker
        CompletedUtc= $CompletedUtc
        UpdatedUtc  = [DateTime]::UtcNow.ToString('o')
    }
    $json = $obj | ConvertTo-Json -Depth 6
    # Write JSON as UTF-8 without BOM for consistency
    $enc = New-Object System.Text.UTF8Encoding($false)
    # Ensure the parent directory exists
    try { $dir = Split-Path -Path $Path -Parent; if ($dir) { New-FTDirectory -Path $dir } } catch { }
    [System.IO.File]::WriteAllText($Path, $json, $enc)
}

function Read-FTRequestFile {
    param([Parameter(Mandatory=$true)][string]$Path)
    $raw = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 -ErrorAction Stop
    return $raw | ConvertFrom-Json -ErrorAction Stop
}

## Read-FTRequests removed (not required)

## Complete-FTRequest removed (not required)

#endregion Helpers

#region Agent

function Start-FTAgent {
    [CmdletBinding()]
    param(
        [Parameter(Position=0)]
        [Alias('Config','Path')]
        [string] $ConfigPath,
        [switch] $Once,
        [Alias('Worker')]
        [string] $AsWorker
    )

    if (-not $ConfigPath -and $args.Count -eq 1 -and -not $Once) { $ConfigPath = $args[0] }

    $cfg = Get-FTConfig -ConfigPath $ConfigPath
    # Validate essential configuration paths early (using resolved paths)
    try {
        $agentPaths = Get-FTPaths -Config $cfg
        if (-not $agentPaths -or [string]::IsNullOrWhiteSpace($agentPaths.Requests) -or [string]::IsNullOrWhiteSpace($agentPaths.Processed)) {
            throw "Invalid configuration: missing Paths.RootPath (Requests/Processed)."
        }
        Ensure-FTDirectories -Config $cfg
    } catch {
        Write-FTLog -Message ("Agent config invalid: {0}" -f $_.Exception.Message) -Level Error -Config $cfg
        return
    }

    $poll = 10
    try {
        if ($cfg.PSObject.Properties['Agent'] -and $cfg.Agent -and $cfg.Agent.PSObject.Properties['PollSeconds']) {
            $poll = [int]$cfg.Agent.PollSeconds
        }
    } catch { $poll = 10 }
    if ($poll -lt 1) { $poll = 10 }

    $thisWorker = if ([string]::IsNullOrWhiteSpace($AsWorker)) { $env:COMPUTERNAME } else { $AsWorker }
    # Optional: warn if worker not listed in configuration Workers
    try {
        if ($cfg.PSObject.Properties['Workers'] -and $cfg.Workers) {
            $rawW = @()
            if ($cfg.Workers -is [hashtable]) { $rawW = @($cfg.Workers.Keys) }
            elseif ($cfg.Workers -is [System.Collections.IEnumerable]) { $rawW = @($cfg.Workers) }
            else { $rawW = @($cfg.Workers) }
            $present = $false
            foreach ($w in $rawW) { if (("" + $w).ToUpperInvariant() -eq ("" + $thisWorker).ToUpperInvariant()) { $present = $true; break } }
            if (-not $present) { Write-FTLog -Message ("Worker '{0}' is not present in configuration Workers list." -f $thisWorker) -Level Warn -Config $cfg }
        }
    } catch { }
    Write-FTLog -Message ("FTAgent monitor starting. Worker={0}, Poll={1} s" -f $thisWorker, $poll) -Level Info -Config $cfg

    do {
        try {
            $agentPaths2 = Get-FTPaths -Config $cfg
            $reqDir  = $agentPaths2.Requests
            $procDir = $agentPaths2.Processed

            New-FTDirectory -Path $reqDir
            New-FTDirectory -Path $procDir

            # Gather counts
            $reqFilter = '*.json'
            try { if ($cfg.Agent -and $cfg.Agent.PSObject.Properties['RequestFileFilter']) { $reqFilter = "" + $cfg.Agent.RequestFileFilter } } catch { $reqFilter = '*.json' }
            $pendingFiles   = @(Get-ChildItem -LiteralPath $reqDir -Filter $reqFilter -File -ErrorAction SilentlyContinue)
            $processedFiles = @(Get-ChildItem -LiteralPath $procDir -Filter *.json -File -ErrorAction SilentlyContinue)
            Write-FTLog -Message ("Agent cycle: pending={0}, processed={1}" -f $pendingFiles.Count, $processedFiles.Count) -Level Trace -Config $cfg

            # Read processing config
            $processEnabled = $true
            $maxParallel = 2
            $maxPerCycle = 10
            try {
                if ($cfg.PSObject.Properties['Agent'] -and $cfg.Agent) {
                    if ($cfg.Agent.PSObject.Properties['Process']) { $processEnabled = [bool]$cfg.Agent.Process }
                    if ($cfg.Agent.PSObject.Properties['MaxParallel']) { $maxParallel = [int]$cfg.Agent.MaxParallel }
                    if ($cfg.Agent.PSObject.Properties['MaxRequestsPerCycle']) { $maxPerCycle = [int]$cfg.Agent.MaxRequestsPerCycle }
                }
            } catch { }

            if ($processEnabled) {
                # Build todo list for this worker, respecting schedule
                $nowUtc = (Get-Date).ToUniversalTime()
                $todo = New-Object System.Collections.ArrayList
                # Sort older first to drain backlog
                $scan = $pendingFiles | Sort-Object LastWriteTimeUtc
                foreach ($f in $scan) {
                    if ($todo.Count -ge $maxPerCycle) { break }
                    try {
                        $req = Read-FTRequestFile -Path $f.FullName
                    } catch { continue }
                    try {
                        $target = "" + $req.Worker
                        if ($target -ne $thisWorker) { continue }
                        $schOk = $true
                        if ($req.PSObject.Properties['ScheduledUtc'] -and $req.ScheduledUtc) {
                            try { $dt = [datetime]$req.ScheduledUtc; if ($dt.ToUniversalTime() -gt $nowUtc) { $schOk = $false } } catch { $schOk = $true }
                        }
                        if (-not $schOk) { continue }
                        $statePath = [System.IO.Path]::ChangeExtension($f.FullName, '.state')
                        if (Test-Path -LiteralPath $statePath) {
                            try {
                                $s = Get-Content -LiteralPath $statePath -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
                                $st = "" + $s.State
                                if ($st -and @('Completed','Failed','InProgress') -contains $st) { continue }
                            } catch { }
                        }
                        [void]$todo.Add([pscustomobject]@{ File=$f; Req=$req })
                    } catch { }
                }

                if ($todo.Count -gt 0) {
                    Write-FTLog -Message ("Processing {0} request(s) with maxParallel={1}" -f $todo.Count, $maxParallel) -Level Info -Config $cfg
                    $moduleFile = Join-Path (Get-FTModuleRoot) 'FSLogixTools.psm1'
                    $jobs = @()
                    $jobScript = {
                        param($reqPath,$processedDir,$worker,$modulePath)
                        try { Import-Module -Force -Name $modulePath -ErrorAction Stop } catch { }
                        $req = $null
                        try { $req = Read-FTRequestFile -Path $reqPath } catch { return }
                        if (("" + $req.Worker) -ne ("" + $worker)) { return }
                        $statePath = [System.IO.Path]::ChangeExtension($reqPath, '.state')
                        try { Write-FTState -Path $statePath -State InProgress -Type 'Request' -Percent 10 -Message 'Processing started' -Worker $worker -RequestId $req.Id } catch { }
                        try {
                            # Simulated processing: move to Processed
                            $dest = Join-Path $processedDir ([System.IO.Path]::GetFileName($reqPath))
                            Move-Item -LiteralPath $reqPath -Destination $dest -Force
                            $destState = [System.IO.Path]::ChangeExtension($dest, '.state')
                            Write-FTState -Path $destState -State Completed -Type 'Request' -Percent 100 -Message 'Completed' -Worker $worker -RequestId $req.Id
                            try { Remove-Item -LiteralPath $statePath -Force -ErrorAction SilentlyContinue } catch { }
                        } catch {
                            try { Write-FTState -Path $statePath -State Failed -Type 'Request' -Percent 100 -Message ("Error: " + $_.Exception.Message) -Worker $worker -RequestId $req.Id } catch { }
                        }
                    }

                    foreach ($it in $todo) {
                        # Throttle
                        while (@($jobs | Where-Object { $_.State -eq 'Running' }).Count -ge $maxParallel) {
                            $done = Wait-Job -Job $jobs -Any -Timeout 2
                            if ($done) { try { Receive-Job -Job $done -ErrorAction SilentlyContinue | Out-Null } catch { } ; try { Remove-Job -Job $done -Force -ErrorAction SilentlyContinue } catch { } }
                            else { break }
                        }
                        $j = Start-Job -ScriptBlock $jobScript -ArgumentList @($it.File.FullName, $procDir, $thisWorker, $moduleFile)
                        $jobs += $j
                    }
                    # Drain
                    while ($jobs.Count -gt 0) {
                        $done2 = Wait-Job -Job $jobs -Any -Timeout 5
                        if ($done2) {
                            try { Receive-Job -Job $done2 -ErrorAction SilentlyContinue | Out-Null } catch { }
                            try { Remove-Job -Job $done2 -Force -ErrorAction SilentlyContinue } catch { }
                            $jobs = @($jobs | Where-Object { $_.Id -ne $done2.Id })
                        } else { break }
                    }
                } else {
                    Write-FTLog -Message "No requests to process for this worker." -Level Trace -Config $cfg
                }
            }
        } catch {
            Write-FTLog -Message ("Agent monitor/process error: {0}" -f $_.Exception.Message) -Level Error -Config $cfg
        }

        if ($Once) { break }
        Start-Sleep -Seconds $poll
    } while ($true)

    Write-FTLog -Message "FTAgent monitor stopped." -Level Info -Config $cfg
}

#endregion Agent

#region GUI (WinForms)

function Show-NewRequestDialog {
    param([object]$Owner,[object]$cfg)

    Add-Type -AssemblyName System.Windows.Forms | Out-Null
    Add-Type -AssemblyName System.Drawing      | Out-Null

    function New-Label([string]$Text,[int]$X,[int]$Y) {
        $lbl = New-Object System.Windows.Forms.Label
        $lbl.Text = $Text
        $lbl.AutoSize = $true
        $lbl.Location = New-Object System.Drawing.Point -ArgumentList $X, $Y
        return $lbl
    }
    function New-Text([int]$X,[int]$Y,[int]$Width) {
        $tb = New-Object System.Windows.Forms.TextBox
        $tb.Location = New-Object System.Drawing.Point -ArgumentList $X, $Y
        $tb.Size     = New-Object System.Drawing.Size  -ArgumentList $Width, 24
        return $tb
    }

    # Resolve Templates directory using RootPath or explicit Templates
    $tplDir = $null
    try {
        $paths = Get-FTPaths -Config $cfg
        if ($paths -and $paths.Templates) { $tplDir = "" + $paths.Templates }
    } catch { $tplDir = $null }
    if (-not $tplDir) {
        $base = $null
        try { if ($cfg -and $cfg.PSObject.Properties['SourcePath'] -and $cfg.SourcePath) { $base = Split-Path -Path $cfg.SourcePath -Parent } } catch { }
        if (-not $base) { $base = Get-FTModuleRoot }
        $tplDir = Join-Path $base 'Templates'
    }

    $dlg = New-Object System.Windows.Forms.Form
    $dlg.Text = (Get-FTText -Config $cfg -Key 'NewRequestTitle' -Default 'New Request')
    $dlg.StartPosition = 'CenterParent'
    $dlg.FormBorderStyle = 'FixedDialog'
    $dlg.MinimizeBox = $false
    $dlg.MaximizeBox = $false
    $dlg.ClientSize = New-Object System.Drawing.Size -ArgumentList 700, 520

    # Request Type (read-only to Template)
    $dlg.Controls.Add((New-Label (Get-FTText -Config $cfg -Key 'RequestTypeLabel' -Default 'Request Type*') 20 20))
    $cmbType = New-Object System.Windows.Forms.ComboBox
    $cmbType.DropDownStyle = 'DropDownList'
    $cmbType.Location = New-Object System.Drawing.Point -ArgumentList 160, 18
    $cmbType.Size     = New-Object System.Drawing.Size  -ArgumentList 520, 24
    $cmbType.Items.AddRange(@('Template')) | Out-Null
    $cmbType.SelectedIndex = 0
    $cmbType.Enabled = $false
    $dlg.Controls.Add($cmbType)

    # Template
    $dlg.Controls.Add((New-Label (Get-FTText -Config $cfg -Key 'TemplateLabel' -Default 'Template*') 20 56))
    $cmbTemplate = New-Object System.Windows.Forms.ComboBox
    $cmbTemplate.DropDownStyle = 'DropDownList'
    $cmbTemplate.Location = New-Object System.Drawing.Point -ArgumentList 160, 54
    $cmbTemplate.Size     = New-Object System.Drawing.Size  -ArgumentList 520, 24
    $cmbTemplate.Sorted   = $false
    $dlg.Controls.Add($cmbTemplate)

    # Worker(s)
    $dlg.Controls.Add((New-Label (Get-FTText -Config $cfg -Key 'WorkersLabel' -Default 'Worker(s)') 20 84))
    $lstWorkers = New-Object System.Windows.Forms.CheckedListBox
    $lstWorkers.Location = New-Object System.Drawing.Point -ArgumentList 160, 112
    $lstWorkers.Size     = New-Object System.Drawing.Size  -ArgumentList 520, 110
    $lstWorkers.Sorted   = $false
    $chkAll = New-Object System.Windows.Forms.CheckBox
    $chkAll.Text = (Get-FTText -Config $cfg -Key 'AllWorkersCheck' -Default 'All')
    $chkAll.Location = New-Object System.Drawing.Point -ArgumentList 160, 88
    $chkAll.AutoSize = $true
    $dlg.Controls.Add($chkAll)
    $dlg.Controls.Add($lstWorkers)
    $ttDlg = New-Object System.Windows.Forms.ToolTip
    try { $ttDlg.SetToolTip($cmbTemplate, (Get-FTTip -Config $cfg -Key 'TemplateCombo' -Default 'Pick a template file')) } catch { }
    try { $ttDlg.SetToolTip($lstWorkers, (Get-FTTip -Config $cfg -Key 'WorkersList' -Default 'Select target workers')) } catch { }
    try { $ttDlg.SetToolTip($chkAll, (Get-FTTip -Config $cfg -Key 'AllWorkersCheck' -Default 'Toggle all workers on/off')) } catch { }
    # Guard to prevent re-entrant toggling between events
    $bulkUpdate = $false
    $chkAll.Add_CheckedChanged({
        if ($bulkUpdate) { return }
        $bulkUpdate = $true
        try {
            for ($i=0; $i -lt $lstWorkers.Items.Count; $i++) { $lstWorkers.SetItemChecked($i, $chkAll.Checked) }
        } finally { $bulkUpdate = $false }
    })
    $lstWorkers.Add_ItemCheck({ param($s,$e)
        if ($bulkUpdate) { return }
        $allChecked = $true
        for ($j=0; $j -lt $lstWorkers.Items.Count; $j++) {
            $isChecked = $lstWorkers.GetItemChecked($j)
            if ($j -eq $e.Index) { $isChecked = ($e.NewValue -eq [System.Windows.Forms.CheckState]::Checked) }
            if (-not $isChecked) { $allChecked = $false; break }
        }
        $bulkUpdate = $true
        try { $chkAll.Checked = $allChecked } finally { $bulkUpdate = $false }
    })

    # Parameters panel
    $dlg.Controls.Add((New-Label (Get-FTText -Config $cfg -Key 'ParametersLabel' -Default 'Parameters') 20 314))
    $pnlParams = New-Object System.Windows.Forms.Panel
    $pnlParams.Location = New-Object System.Drawing.Point -ArgumentList 160, 332
    $pnlParams.Size     = New-Object System.Drawing.Size  -ArgumentList 520, 110
    $pnlParams.AutoScroll = $true
    $dlg.Controls.Add($pnlParams)
    try { $ttDlg.SetToolTip($pnlParams, (Get-FTTip -Config $cfg -Key 'ParametersPanel' -Default 'Template-defined parameters')) } catch { }
    $script:paramControls = @{}

    # Scheduled / Priority row
    $dlg.Controls.Add((New-Label (Get-FTText -Config $cfg -Key 'ScheduledLabel' -Default 'Scheduled (UTC)') 20 244))
    $dtp = New-Object System.Windows.Forms.DateTimePicker
    $dtp.Location = New-Object System.Drawing.Point -ArgumentList 160, 244
    $dtp.Size     = New-Object System.Drawing.Size  -ArgumentList 240, 24
    $dtp.Format   = [System.Windows.Forms.DateTimePickerFormat]::Custom
    $dtp.CustomFormat = "yyyy-MM-dd HH:mm:ss"
    $dtp.ShowCheckBox = $true
    $dtp.Checked = $false
    $dlg.Controls.Add($dtp)
    try { $ttDlg.SetToolTip($dtp, (Get-FTTip -Config $cfg -Key 'DateTime' -Default 'Optional UTC schedule')) } catch { }

    $dlg.Controls.Add((New-Label (Get-FTText -Config $cfg -Key 'PriorityLabel' -Default 'Priority') 420 244))
    $cmbPriority = New-Object System.Windows.Forms.ComboBox
    $cmbPriority.DropDownStyle = 'DropDownList'
    $cmbPriority.Location = New-Object System.Drawing.Point -ArgumentList 480, 244
    $cmbPriority.Size     = New-Object System.Drawing.Size  -ArgumentList 200, 24
    $cmbPriority.Sorted   = $false
    try {
        $prio = @('Low','Normal','High')
        if ($cfg.PSObject.Properties['Gui'] -and $cfg.Gui -and $cfg.Gui.PSObject.Properties['Priorities']) {
            $rawP = @($cfg.Gui.Priorities)
            $txtP = @(); foreach ($p in $rawP) { $s = "" + $p; if (-not [string]::IsNullOrWhiteSpace($s)) { $txtP += $s } }
            if ($txtP.Count -gt 0) { $prio = $txtP }
        }
        $cmbPriority.Items.AddRange(@($prio)) | Out-Null
        $idxNorm = [Array]::IndexOf($cmbPriority.Items, 'Normal')
        if ($idxNorm -ge 0) { $cmbPriority.SelectedIndex = $idxNorm } elseif ($cmbPriority.Items.Count -gt 0) { $cmbPriority.SelectedIndex = 0 }
    } catch { $cmbPriority.Items.AddRange(@('Low','Normal','High')) | Out-Null; $cmbPriority.SelectedIndex = 1 }
    $dlg.Controls.Add($cmbPriority)
    try { $ttDlg.SetToolTip($cmbPriority, (Get-FTTip -Config $cfg -Key 'Priority' -Default 'Request priority')) } catch { }

    # Notes row
    $dlg.Controls.Add((New-Label (Get-FTText -Config $cfg -Key 'NotesLabel' -Default 'Notes') 20 276))
    $txtNotes = New-Text 160 274 520
    $dlg.Controls.Add($txtNotes)
    try { $ttDlg.SetToolTip($txtNotes, (Get-FTTip -Config $cfg -Key 'Notes' -Default 'Additional notes')) } catch { }

    # Buttons
    $btnOK = New-Object System.Windows.Forms.Button
    $btnOK.Text = (Get-FTText -Config $cfg -Key 'CreateButton' -Default 'Create')
    $btnOK.Location = New-Object System.Drawing.Point -ArgumentList 560, 458
    $btnOK.Size     = New-Object System.Drawing.Size  -ArgumentList 120, 30
    $dlg.Controls.Add($btnOK)
    try { $ttDlg.SetToolTip($btnOK, (Get-FTTip -Config $cfg -Key 'CreateButton' -Default 'Create the request(s)')) } catch { }

    $btnCancel = New-Object System.Windows.Forms.Button
    $btnCancel.Text = (Get-FTText -Config $cfg -Key 'CancelButton' -Default 'Cancel')
    $btnCancel.Location = New-Object System.Drawing.Point -ArgumentList 430, 458
    $btnCancel.Size     = New-Object System.Drawing.Size  -ArgumentList 120, 30
    $btnCancel.Add_Click({ $dlg.DialogResult = [System.Windows.Forms.DialogResult]::Cancel; $dlg.Close() })
    $dlg.Controls.Add($btnCancel)
    try { $ttDlg.SetToolTip($btnCancel, (Get-FTTip -Config $cfg -Key 'CancelButton' -Default 'Close dialog without creating')) } catch { }

    # Render parameters function (from template)
    $renderParams = {
        param([object]$tmplObjIn)
        if (-not $pnlParams) { return }
        try {
            $pnlParams.SuspendLayout()
            $pnlParams.Controls.Clear()
            $script:paramControls = @{}
            $y = 0
            if ($tmplObjIn -and $tmplObjIn.PSObject.Properties['ParameterDefs'] -and $tmplObjIn.ParameterDefs) {
                foreach ($def in $tmplObjIn.ParameterDefs) {
                    $name  = "" + $def.Name
                    if ([string]::IsNullOrWhiteSpace($name)) { continue }
                    $type  = 'string'; if ($def.PSObject.Properties['Type']) { $type = ("" + $def.Type).ToLowerInvariant() }
                    $label = $name; if ($def.PSObject.Properties['Label'] -and $def.Label) { $label = "" + $def.Label }
                    $dflt  = $null
                    if ($def.PSObject.Properties['Default']) { $dflt = $def.Default }
                    elseif ($tmplObjIn.PSObject.Properties['Parameters'] -and $tmplObjIn.Parameters -and $tmplObjIn.Parameters.PSObject.Properties[$name]) { $dflt = $tmplObjIn.Parameters.$name }
                    $lbl = New-Object System.Windows.Forms.Label
                    $lbl.Text = $label
                    $lbl.AutoSize = $true
                    $lbl.Location = New-Object System.Drawing.Point -ArgumentList 0, ($y + 4)
                    [void]$pnlParams.Controls.Add($lbl)
                    switch ($type) {
                        'bool' {
                            $ctl = New-Object System.Windows.Forms.CheckBox
                            $ctl.Location = New-Object System.Drawing.Point -ArgumentList 200, $y
                            $ctl.AutoSize = $true
                            if ($null -ne $dflt) { $ctl.Checked = [bool]$dflt }
                            [void]$pnlParams.Controls.Add($ctl)
                            $script:paramControls[$name] = @{ Type='bool'; Control=$ctl }
                            $y += 28
                        }
                        'int' {
                            $ctl = New-Object System.Windows.Forms.NumericUpDown
                            $ctl.Location = New-Object System.Drawing.Point -ArgumentList 200, $y
                            $ctl.Size     = New-Object System.Drawing.Size  -ArgumentList 200, 24
                            $ctl.Minimum  = -2147483648
                            $ctl.Maximum  =  2147483647
                            if ($def.PSObject.Properties['Min']) { try { $ctl.Minimum = [decimal]$def.Min } catch {} }
                            if ($def.PSObject.Properties['Max']) { try { $ctl.Maximum = [decimal]$def.Max } catch {} }
                            if ($null -ne $dflt) { try { $ctl.Value = [decimal]$dflt } catch {} }
                            [void]$pnlParams.Controls.Add($ctl)
                            $script:paramControls[$name] = @{ Type='int'; Control=$ctl }
                            $y += 28
                        }
                        'number' {
                            $ctl = New-Object System.Windows.Forms.NumericUpDown
                            $ctl.Location = New-Object System.Drawing.Point -ArgumentList 200, $y
                            $ctl.Size     = New-Object System.Drawing.Size  -ArgumentList 200, 24
                            $ctl.DecimalPlaces = 2
                            $ctl.Minimum  = -1000000000
                            $ctl.Maximum  =  1000000000
                            if ($def.PSObject.Properties['Min']) { try { $ctl.Minimum = [decimal]$def.Min } catch {} }
                            if ($def.PSObject.Properties['Max']) { try { $ctl.Maximum = [decimal]$def.Max } catch {} }
                            if ($null -ne $dflt) { try { $ctl.Value = [decimal]$dflt } catch {} }
                            [void]$pnlParams.Controls.Add($ctl)
                            $script:paramControls[$name] = @{ Type='number'; Control=$ctl }
                            $y += 28
                        }
                        'select' {
                            $ctl = New-Object System.Windows.Forms.ComboBox
                            $ctl.DropDownStyle = 'DropDownList'
                            $ctl.Location = New-Object System.Drawing.Point -ArgumentList 200, $y
                            $ctl.Size     = New-Object System.Drawing.Size  -ArgumentList 200, 24
                            $choices = @()
                            if ($def.PSObject.Properties['Choices'] -and $def.Choices) { $choices = @($def.Choices) }
                            if ($choices.Count -gt 0) { $ctl.Items.AddRange(@($choices | ForEach-Object { "" + $_ })) | Out-Null }
                            if ($null -ne $dflt) {
                                $d = "" + $dflt
                                $idx = $ctl.Items.IndexOf($d)
                                if ($idx -ge 0) { $ctl.SelectedIndex = $idx }
                            } elseif ($ctl.Items.Count -gt 0) {
                                $ctl.SelectedIndex = 0
                            }
                            [void]$pnlParams.Controls.Add($ctl)
                            $script:paramControls[$name] = @{ Type='select'; Control=$ctl }
                            $y += 28
                        }
                        'multiselect' {
                            $ctl = New-Object System.Windows.Forms.CheckedListBox
                            $ctl.Location = New-Object System.Drawing.Point -ArgumentList 200, $y
                            $ctl.Size     = New-Object System.Drawing.Size  -ArgumentList 200, 60
                            $ctl.CheckOnClick = $true
                            $choices = @()
                            if ($def.PSObject.Properties['Choices'] -and $def.Choices) { $choices = @($def.Choices) }
                            if ($choices.Count -gt 0) { $ctl.Items.AddRange(@($choices | ForEach-Object { "" + $_ })) | Out-Null }
                            if ($null -ne $dflt) {
                                foreach ($v in @($dflt)) {
                                    $idx = $ctl.Items.IndexOf(("" + $v))
                                    if ($idx -ge 0) { $ctl.SetItemChecked($idx, $true) }
                                }
                            }
                            [void]$pnlParams.Controls.Add($ctl)
                            $script:paramControls[$name] = @{ Type='multiselect'; Control=$ctl }
                            $y += 68
                        }
                        'datetime' {
                            $ctl = New-Object System.Windows.Forms.DateTimePicker
                            $ctl.Location = New-Object System.Drawing.Point -ArgumentList 200, $y
                            $ctl.Size     = New-Object System.Drawing.Size  -ArgumentList 200, 24
                            $ctl.Format   = [System.Windows.Forms.DateTimePickerFormat]::Custom
                            $ctl.CustomFormat = "yyyy-MM-dd HH:mm:ss"
                            if ($null -ne $dflt) { try { $ctl.Value = [datetime]$dflt } catch {} }
                            [void]$pnlParams.Controls.Add($ctl)
                            $script:paramControls[$name] = @{ Type='datetime'; Control=$ctl }
                            $y += 28
                        }
                        default {
                            $ctl = New-Object System.Windows.Forms.TextBox
                            $ctl.Location = New-Object System.Drawing.Point -ArgumentList 200, $y
                            $ctl.Size     = New-Object System.Drawing.Size  -ArgumentList 200, 24
                            if ($null -ne $dflt) { $ctl.Text = "" + $dflt }
                            [void]$pnlParams.Controls.Add($ctl)
                            $script:paramControls[$name] = @{ Type='string'; Control=$ctl }
                            $y += 28
                        }
                    }
                }
            } else {
    $lbl = New-Object System.Windows.Forms.Label
    $lbl.Text = (Get-FTText -Config $cfg -Key 'NoParametersDefined' -Default 'No parameters defined.')
                $lbl.AutoSize = $true
                $lbl.Location = New-Object System.Drawing.Point -ArgumentList 0, 0
                [void]$pnlParams.Controls.Add($lbl)
            }
        } catch {
            $pnlParams.Controls.Clear()
            $lblErr = New-Object System.Windows.Forms.Label
            $lblErr.Text = (Get-FTText -Config $cfg -Key 'TemplateLoadFailed' -Default 'Template load failed: {0}').Replace('{0}', $_.Exception.Message)
            $lblErr.AutoSize = $true
            $lblErr.ForeColor = [System.Drawing.Color]::DarkRed
            $lblErr.Location = New-Object System.Drawing.Point -ArgumentList 0, 0
            [void]$pnlParams.Controls.Add($lblErr)
        } finally { try { $pnlParams.ResumeLayout() } catch { } }
    }

    # Populate template list and initial parameters
    try {
        if (Test-Path -LiteralPath $tplDir) {
            $files = Get-ChildItem -LiteralPath $tplDir -Filter '*.request.json' -File -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Name
        } else { $files = @() }
        if (-not $files -or $files.Count -eq 0) {
            try {
                New-Item -ItemType Directory -Path $tplDir -Force | Out-Null
                $sample1 = @{ Operation = 'Template'; ParameterDefs = @(@{ Name='Name'; Type='string'; Label='Name'; Default='Demo' }) ; Parameters=@{} } | ConvertTo-Json -Depth 5
                [System.IO.File]::WriteAllText((Join-Path $tplDir 'Sample.request.json'), $sample1, (New-Object System.Text.UTF8Encoding($false)))
            } catch { }
            $files = Get-ChildItem -LiteralPath $tplDir -Filter '*.request.json' -File -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Name
        }
        if ($files) { $cmbTemplate.Items.AddRange(@($files)) | Out-Null }
    } catch { }

    $cmbTemplate.Add_SelectedIndexChanged({
        try {
            $sel = "" + $cmbTemplate.SelectedItem
            if ([string]::IsNullOrWhiteSpace($sel)) { return }
            $file = Join-Path $tplDir $sel
            if (-not (Test-Path -LiteralPath $file)) { return }
            $tmplObj = Get-Content -LiteralPath $file -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
            & $renderParams $tmplObj
        } catch {
            $pnlParams.Controls.Clear()
            $lblErr = New-Object System.Windows.Forms.Label
            $lblErr.Text = (Get-FTText -Config $cfg -Key 'TemplateLoadFailed' -Default 'Template load failed: {0}').Replace('{0}', $_.Exception.Message)
            $lblErr.AutoSize = $true
            $lblErr.ForeColor = [System.Drawing.Color]::DarkRed
            $lblErr.Location = New-Object System.Drawing.Point -ArgumentList 0, 0
            [void]$pnlParams.Controls.Add($lblErr)
        }
    })

    $dlg.Add_Shown({
        try {
            if ($cmbTemplate.Items.Count -gt 0 -and -not $cmbTemplate.SelectedItem) { $cmbTemplate.SelectedIndex = 0 }
            $sel = "" + $cmbTemplate.SelectedItem
            if ([string]::IsNullOrWhiteSpace($sel)) { return }
            $file = Join-Path $tplDir $sel
            if (-not (Test-Path -LiteralPath $file)) { return }
            $tmplObj = Get-Content -LiteralPath $file -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
            & $renderParams $tmplObj
        } catch {
            $pnlParams.Controls.Clear()
            $lblErr = New-Object System.Windows.Forms.Label
            $lblErr.Text = "Template load failed: " + $_.Exception.Message
            $lblErr.AutoSize = $true
            $lblErr.ForeColor = [System.Drawing.Color]::DarkRed
            $lblErr.Location = New-Object System.Drawing.Point -ArgumentList 0, 0
            [void]$pnlParams.Controls.Add($lblErr)
        }
    })

    # Fill workers from config
    try {
        $list = @()
        if ($cfg.PSObject.Properties['Workers'] -and $cfg.Workers) {
            $raw = @()
            if ($cfg.Workers -is [hashtable]) { $raw = @($cfg.Workers.Keys) }
            elseif ($cfg.Workers -is [System.Collections.IEnumerable]) { $raw = @($cfg.Workers) }
            else { $raw = @($cfg.Workers) }

            $expandEnv = { param([string]$v)
                if ($v -match '(?i)^\s*\$env:([A-Za-z0-9_]+)\s*$') {
                    $n = $Matches[1]
                    $val = [Environment]::GetEnvironmentVariable($n)
                    if ($val) { return $val } else { return "" }
                }
                return $v
            }

            $list = @($raw | ForEach-Object { & $expandEnv ("" + $_) } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        } else {
            # strict: use only what is in configuration
            $list = @()
        }
        $seen = @{}
        foreach ($w in $list) {
            $s = "" + $w
            if (-not $seen.ContainsKey($s)) { $seen[$s] = $true; [void]$lstWorkers.Items.Add($s) }
        }
        if ($lstWorkers.Items.Count -gt 0) { $lstWorkers.SetItemChecked(0,$true) }
    } catch { }

    $btnOK.Add_Click({
        $groupId = [guid]::NewGuid().Guid
        try {
            $reqType = "" + $cmbType.SelectedItem
            if ([string]::IsNullOrWhiteSpace($reqType)) { throw "Request Type is required." }

            # Build Parameters from UI
            $paramsObj = @{}
            if ($script:paramControls -and ($script:paramControls.Keys.Count -gt 0)) {
                # iterate by stable list of keys to avoid any comparer usage on DictionaryEntry
                $names = @()
                foreach ($k in $script:paramControls.Keys) { $names += ("" + $k) }
                foreach ($name in $names) {
                    $meta = $script:paramControls[$name]; $ctl = $meta.Control; $type = "" + $meta.Type
                    $val = $null
                    switch ($type) {
                        'bool'        { $val = [bool]$ctl.Checked }
                        'int'         { $val = [int][decimal]$ctl.Value }
                        'number'      { $val = [double][decimal]$ctl.Value }
                        'select'      { $val = "" + $ctl.SelectedItem }
                        'multiselect' { $vals = @(); foreach ($ci in $ctl.CheckedItems) { $vals += "" + $ci }; $val = $vals }
                        'datetime'    { $val = $ctl.Value.ToString("o") }
                        default       { $val = "" + $ctl.Text }
                    }
                    $paramsObj[$name] = $val
                }
            } else {
                # fallback from template default Parameters
                try {
                    $sel = "" + $cmbTemplate.SelectedItem
                    if (-not [string]::IsNullOrWhiteSpace($sel)) {
                        $tmplFile = Join-Path $tplDir $sel
                        if (Test-Path -LiteralPath $tmplFile) {
                            $tmplObj = Get-Content -LiteralPath $tmplFile -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop
                            if ($tmplObj.Parameters) {
                                foreach ($p in $tmplObj.Parameters.PSObject.Properties) {
                                    $paramsObj[$p.Name] = $p.Value
                                }
                            }
                        }
                    }
                } catch { }
            }
            $paramsObj = [pscustomobject]$paramsObj

            # schedule
            $scheduledUtc = $null
            if ($dtp.Checked) { $scheduledUtc = $dtp.Value.ToUniversalTime().ToString("o") }

            # workers (build via index to avoid odd enumerable behaviors)
            $targets = @()
            if ($chkAll.Checked) {
                for ($i=0; $i -lt $lstWorkers.Items.Count; $i++) { $targets += ("" + $lstWorkers.Items[$i]) }
            } else {
                for ($i=0; $i -lt $lstWorkers.Items.Count; $i++) {
                    if ($lstWorkers.GetItemChecked($i)) { $targets += ("" + $lstWorkers.Items[$i]) }
                }
            }
            if ($targets.Count -eq 0) { throw "Select at least one worker (or All)." }

            $reqDir = (Get-FTPaths -Config $cfg).Requests
            New-FTDirectory -Path $reqDir

            foreach ($w in $targets) {
                $priority = 'Normal'
                try { if ($cmbPriority -and $cmbPriority.SelectedItem) { $priority = "" + $cmbPriority.SelectedItem } } catch { }
                # Prepare additional metadata values outside of the hashtable
                $cfgPathValue = $null
                $cfgVerValue  = $null
                try { if ($cfg.PSObject.Properties['SourcePath']) { $cfgPathValue = "" + $cfg.SourcePath } } catch { }
                try { if ($cfg.PSObject.Properties['Version'])    { $cfgVerValue  = "" + $cfg.Version } } catch { }
                $filePattern = '{0}.request.json'
                try {
                    if ($cfg.PSObject.Properties['Agent'] -and $cfg.Agent -and $cfg.Agent.PSObject.Properties['RequestFilePattern']) {
                        $filePattern = "" + $cfg.Agent.RequestFilePattern
                    }
                } catch { $filePattern = '{0}.request.json' }
                $req = [pscustomobject]@{
                    Id           = [guid]::NewGuid().Guid
                    GroupId      = $groupId
                    Operation    = $reqType
                    Parameters   = $paramsObj
                    Notes        = $txtNotes.Text
                    RequestedBy  = $env:USERNAME
                    Priority     = $priority
                    ScheduledUtc = $scheduledUtc
                    Worker       = $w
                    CreatedUtc   = (Get-Date).ToUniversalTime().ToString("o")
                    SourceComputer = $env:COMPUTERNAME
                    ConfigPath     = $cfgPathValue
                    ConfigVersion  = $cfgVerValue
                }
                $jsonOut = $req | ConvertTo-Json -Depth 10
                $enc = New-Object System.Text.UTF8Encoding($false)
                $path = Join-Path $reqDir ($filePattern -f $req.Id)
                [System.IO.File]::WriteAllText($path, $jsonOut, $enc)
                # Create initial state file (New)
                try {
                    $statePath = [System.IO.Path]::ChangeExtension($path, '.state')
                    Write-FTState -Path $statePath -State New -Type 'Request' -Percent 0 -Message 'Created' -Worker $w -RequestId $req.Id
                } catch { }
            }

            [System.Windows.Forms.MessageBox]::Show((Get-FTText -Config $cfg -Key 'NewRequestCreatedInfo' -Default 'Request(s) created: {0}').Replace('{0}', (""+$targets.Count)), (Get-FTText -Config $cfg -Key 'AppTitle' -Default 'FSLogixTools'),
                [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Information) | Out-Null
            $dlg.DialogResult = [System.Windows.Forms.DialogResult]::OK
            $dlg.Close()
        } catch {
            try { Write-FTLog -Message ("Create NewRequest failed: {0}" -f $_.Exception.ToString()) -Level Error -Config $cfg } catch { }
            $detail = try { "`r`n`r`n" + ("" + $_.ScriptStackTrace) } catch { "" }
            [System.Windows.Forms.MessageBox]::Show((Get-FTText -Config $cfg -Key 'CreateFailedMessage' -Default 'Create failed: {0}').Replace('{0}', $_.Exception.Message) + $detail, (Get-FTText -Config $cfg -Key 'CreateFailedTitle' -Default 'FSLogixTools - Error'),
                [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Error) | Out-Null
        }
    })

    if ($Owner) { $dlg.ShowDialog($Owner) | Out-Null } else { $dlg.ShowDialog() | Out-Null }
}

function Start-FTGui {
    [CmdletBinding()]
    param(
        [switch] $Browse,
        [Parameter(Position=0)]
        [Alias('Config','Path')]
        [string] $ConfigPath
    )

    Add-Type -AssemblyName System.Windows.Forms
    Add-Type -AssemblyName System.Drawing

    $cfg = $null
    if ($Browse) {
        # Let the user pick a JSON config file
        $dlgCfg = New-Object System.Windows.Forms.OpenFileDialog
        $dlgCfg.Title = 'Select FSLogixTools configuration (JSON)'
        $dlgCfg.Filter = 'JSON files (*.json)|*.json|All files (*.*)|*.*'
        try {
            $initDir = $null
            if ($ConfigPath) { $initDir = Split-Path -Path $ConfigPath -Parent }
            elseif (Test-Path -LiteralPath (Join-Path (Get-FTModuleRoot) 'FSLogixTools.json')) { $initDir = (Get-FTModuleRoot) }
            else { $initDir = (Get-Location).Path }
            if ($initDir) { $dlgCfg.InitialDirectory = $initDir }
        } catch { }
        if ($dlgCfg.ShowDialog() -ne [System.Windows.Forms.DialogResult]::OK) { return }
        $ConfigPath = $dlgCfg.FileName
    }
    try { $cfg = Get-FTConfig -ConfigPath $ConfigPath } catch { $cfg = $null }
    if ($cfg) { try { $cfg = Normalize-FTConfig -Config $cfg } catch { } }
    if (-not $cfg) {
        [System.Windows.Forms.MessageBox]::Show(
            (Get-FTText -Config $cfg -Key 'ConfigNotFoundMsg' -Default 'Configuration file not found or failed to load.'),
            (Get-FTText -Config $cfg -Key 'AppTitle' -Default 'FSLogixTools'),
            [System.Windows.Forms.MessageBoxButtons]::OK,
            [System.Windows.Forms.MessageBoxIcon]::Error
        ) | Out-Null
        return
    }

    # Validate essential configuration paths early (using resolved paths)
    $missing = @()
    $paths = $null
    try {
        $paths = Get-FTPaths -Config $cfg
        if (-not $paths -or [string]::IsNullOrWhiteSpace($paths.Requests)) { $missing += 'Paths.RootPath (Requests)' }
        if (-not $paths -or [string]::IsNullOrWhiteSpace($paths.Processed)) { $missing += 'Paths.RootPath (Processed)' }
    } catch { $missing += 'Paths.RootPath' }
    if ($missing.Count -gt 0) {
        [System.Windows.Forms.MessageBox]::Show(
            (Get-FTText -Config $cfg -Key 'InvalidConfigMsg' -Default 'Invalid configuration. Missing: {0}').Replace('{0}', ($missing -join ', ')),
            (Get-FTText -Config $cfg -Key 'AppTitle' -Default 'FSLogixTools'),
            [System.Windows.Forms.MessageBoxButtons]::OK,
            [System.Windows.Forms.MessageBoxIcon]::Error
        ) | Out-Null
        return
    }
    # Ensure directories exist
    Ensure-FTDirectories -Config $cfg

    $form              = New-Object System.Windows.Forms.Form
    try { $form.Text = Get-FTText -Config $cfg -Key 'AppTitle' -Default 'FSLogixTools' } catch { $form.Text = 'FSLogixTools' }
    $form.StartPosition= "CenterScreen"
    try {
        $w = 1260; $h = 680
        if ($cfg.PSObject.Properties['Gui'] -and $cfg.Gui) {
            if ($cfg.Gui.PSObject.Properties['Width'])  { $w = [int]$cfg.Gui.Width }
            if ($cfg.Gui.PSObject.Properties['Height']) { $h = [int]$cfg.Gui.Height }
        }
        $form.Size = New-Object System.Drawing.Size($w,$h)
    } catch { $form.Size = New-Object System.Drawing.Size(1260,680) }
    # TopMost controlled by configuration (default: false)
    try {
        $topMost = $false
        if ($cfg.PSObject.Properties['Gui'] -and $cfg.Gui -and $cfg.Gui.PSObject.Properties['TopMost']) {
            $topMost = [bool]$cfg.Gui.TopMost
        }
        $form.TopMost = $topMost
    } catch { $form.TopMost = $false }

    $btnRefresh = New-Object System.Windows.Forms.Button
    $btnRefresh.Text = (Get-FTText -Config $cfg -Key 'RefreshButton' -Default 'Refresh')
    $btnRefresh.Location = New-Object System.Drawing.Point(10, 10)
    $btnRefresh.Size = New-Object System.Drawing.Size(90, 28)
    $form.Controls.Add($btnRefresh)
    # ToolTips for main controls
    $tt = New-Object System.Windows.Forms.ToolTip
    try { $tt.SetToolTip($btnRefresh, (Get-FTTip -Config $cfg -Key 'RefreshButton' -Default 'Reload the queue view')) } catch { }

    $btnNew = New-Object System.Windows.Forms.Button
    $btnNew.Text = (Get-FTText -Config $cfg -Key 'NewRequestButton' -Default 'New Request')
    $btnNew.Location = New-Object System.Drawing.Point(110, 10)
    $btnNew.Size = New-Object System.Drawing.Size(120, 28)
    $form.Controls.Add($btnNew)
    try { $tt.SetToolTip($btnNew, (Get-FTTip -Config $cfg -Key 'NewRequestButton' -Default 'Create a new request')) } catch { }

    $lblState = New-Object System.Windows.Forms.Label
    $lblState.Text = (Get-FTText -Config $cfg -Key 'StateLabel' -Default 'State:')
    $lblState.Location = New-Object System.Drawing.Point(410, 15)
    $lblState.AutoSize = $true
    $form.Controls.Add($lblState)

    $cbState = New-Object System.Windows.Forms.ComboBox
    $cbState.DropDownStyle = 'DropDownList'
    $cbState.Location = New-Object System.Drawing.Point(455, 12)
    $cbState.Size = New-Object System.Drawing.Size(140, 24)
    $cbState.Items.AddRange(@("All","New","Queued","InProgress","Completed","Failed")) | Out-Null
    $cbState.SelectedIndex = 0
    $form.Controls.Add($cbState)
    try { $tt.SetToolTip($cbState, (Get-FTTip -Config $cfg -Key 'StateFilter' -Default 'Filter the queue by state')) } catch { }

    $lblWorker = New-Object System.Windows.Forms.Label
    $lblWorker.Text = (Get-FTText -Config $cfg -Key 'WorkerLabel' -Default 'Worker:')
    $lblWorker.Location = New-Object System.Drawing.Point(605, 15)
    $lblWorker.AutoSize = $true
    $form.Controls.Add($lblWorker)

    $cbWorker = New-Object System.Windows.Forms.ComboBox
    $cbWorker.DropDownStyle = 'DropDownList'
    $cbWorker.Location = New-Object System.Drawing.Point(660, 12)
    $cbWorker.Size = New-Object System.Drawing.Size(160, 24)
    $cbWorker.Items.AddRange(@("All")) | Out-Null
    $cbWorker.SelectedIndex = 0
    $form.Controls.Add($cbWorker)
    try { $tt.SetToolTip($cbWorker, (Get-FTTip -Config $cfg -Key 'WorkerFilter' -Default 'Filter the queue by worker')) } catch { }

    
# Auto refresh controls
$lblAuto = New-Object System.Windows.Forms.Label
$lblAuto.Text = (Get-FTText -Config $cfg -Key 'AutoLabel' -Default 'Auto:'); $lblAuto.AutoSize = $true
$lblAuto.Location = New-Object System.Drawing.Point(820, 16)
$lblAuto.Anchor = [System.Windows.Forms.AnchorStyles]::Top -bor [System.Windows.Forms.AnchorStyles]::Left
$form.Controls.Add($lblAuto)

$cbAuto = New-Object System.Windows.Forms.ComboBox
$cbAuto.DropDownStyle = 'DropDownList'
$cbAuto.Location = New-Object System.Drawing.Point(870, 12)
$cbAuto.Size = New-Object System.Drawing.Size(90, 24)
$cbAuto.Anchor = [System.Windows.Forms.AnchorStyles]::Top -bor [System.Windows.Forms.AnchorStyles]::Left
# Options from configuration; prefer Gui.AutoOptions, fallback to Gui.AutoRefreshOptions
$cbAuto.Items.Clear()
$autoOpts = @(0,5,10,30,60)
try {
    $raw = @()
    if ($cfg.PSObject.Properties['Gui'] -and $cfg.Gui -and $cfg.Gui.PSObject.Properties['AutoOptions']) { $raw = @($cfg.Gui.AutoOptions) }
    elseif ($cfg.PSObject.Properties['Gui'] -and $cfg.Gui -and $cfg.Gui.PSObject.Properties['AutoRefreshOptions']) { $raw = @($cfg.Gui.AutoRefreshOptions) }
    $ints = @(); foreach ($v in $raw) { try { $ints += [int]$v } catch { } }
    if ($ints.Count -gt 0) { $autoOpts = $ints }
} catch { }
$labels = @()
foreach ($sec in $autoOpts) { if ([int]$sec -le 0) { $labels += 'Off' } else { $labels += ("{0} s" -f [int]$sec) } }
$labels = @($labels | Select-Object -Unique)
$cbAuto.Items.AddRange(@($labels)) | Out-Null
$cbAuto.SelectedIndex = 0
$form.Controls.Add($cbAuto)
try { $tt.SetToolTip($cbAuto, (Get-FTTip -Config $cfg -Key 'AutoRefresh' -Default 'Select auto refresh interval')) } catch { }

$timer = New-Object System.Windows.Forms.Timer
$timer.Interval = 10000
$timer.Add_Tick({ try { $timer.Stop() } catch { } ; try { & $loadQueue } finally { try { if ($cbAuto.SelectedItem -and ($cbAuto.SelectedItem -ne 'Off')) { $timer.Start() } } catch { } } })

    # Default auto refresh value from configuration (no legacy fallbacks)
    try {
        $defaultSecs = 0
        if ($cfg.PSObject.Properties['Gui'] -and $cfg.Gui -and $cfg.Gui.PSObject.Properties['AutoRefreshSeconds']) {
            $defaultSecs = [int]$cfg.Gui.AutoRefreshSeconds
        }
        # select appropriate item; if not present, set 'Off'
        $target = if ($defaultSecs -le 0) { 'Off' } else { ("{0} s" -f $defaultSecs) }
        if (($cbAuto.Items | ForEach-Object { "" + $_ }) -contains $target) { $cbAuto.SelectedItem = $target } else { $cbAuto.SelectedItem = 'Off' }
        # configure and start/stop the timer based on the selected value
        $sel = "" + $cbAuto.SelectedItem
        if ($sel -eq 'Off') { $timer.Stop() }
        elseif ($sel -match '^(\d+) s$') { $timer.Interval = [int]$Matches[1] * 1000; $timer.Start() } else { $timer.Stop() }
    } catch { }

$cbAuto.Add_SelectedIndexChanged({
    $sel2 = "" + $cbAuto.SelectedItem
    if ($sel2 -eq 'Off') { $timer.Stop() }
    elseif ($sel2 -match '^(\d+) s$') { $timer.Interval = [int]$Matches[1] * 1000; $timer.Start() } else { $timer.Stop() }
})
$lblQueue = New-Object System.Windows.Forms.Label
    $lblQueue.Text = "Queued Requests (incl. processed)"
    $lblQueue.AutoSize = $true
    $lblQueue.Location = New-Object System.Drawing.Point(10, 50)
    $form.Controls.Add($lblQueue)

    $grid = New-Object System.Windows.Forms.DataGridView
    $grid.Location = New-Object System.Drawing.Point(10, 70)
    $grid.Size     = New-Object System.Drawing.Size(1220, 540)
    $grid.ReadOnly = $true
    $grid.AllowUserToAddRows = $false
    $grid.AllowUserToDeleteRows = $false
    $grid.AutoSizeColumnsMode = 'Fill'
    $grid.AutoGenerateColumns = $false

    # Columns from config if present, else defaults
    $columnsCfg = $null
    try { if ($cfg.PSObject.Properties['Gui'] -and $cfg.Gui -and $cfg.Gui.PSObject.Properties['Columns']) { $columnsCfg = @($cfg.Gui.Columns) } } catch { $columnsCfg = $null }
    if (-not $columnsCfg -or $columnsCfg.Count -eq 0) {
        $columnsCfg = @(
            @{Name='GroupId';Header='GroupId';Width=230},
            @{Name='Id';Header='Id';Width=230},
            @{Name='State';Header='State';Width=110},
            @{Name='Message';Header='Message';Width=260},
            @{Name='Operation';Header='Operation';Width=120},
            @{Name='Worker';Header='Worker';Width=120},
            @{Name='Priority';Header='Priority';Width=100},
            @{Name='RequestedBy';Header='Requested By';Width=120},
            @{Name='Created';Header='Created';Width=150},
            @{Name='Scheduled';Header='Scheduled';Width=150},
            @{Name='Completed';Header='Completed';Width=150}
        )
    }
    foreach ($col in $columnsCfg) {
        $c = New-Object System.Windows.Forms.DataGridViewTextBoxColumn
        $name = try { "" + $col.Name } catch { "" }
        $header = try { "" + $col.Header } catch { $name }
        $width = try { [int]$col.Width } catch { 100 }
        if ([string]::IsNullOrWhiteSpace($name)) { continue }
        $c.Name = $name
        $c.HeaderText = $header
        $c.DataPropertyName = $name
        $c.Width = $width
        $grid.Columns.Add($c) | Out-Null
    }
$script:FT_columns = @($columnsCfg | ForEach-Object { try { "" + $_.Name } catch { $_ } })

    $form.Controls.Add($grid)
    try { $tt.SetToolTip($grid, (Get-FTTip -Config $cfg -Key 'Grid' -Default 'Queue overview')) } catch { }

    $status = New-Object System.Windows.Forms.StatusStrip
    $sbAll = New-Object System.Windows.Forms.ToolStripStatusLabel
    $sbShown = New-Object System.Windows.Forms.ToolStripStatusLabel
    $sbInProg = New-Object System.Windows.Forms.ToolStripStatusLabel
    $status.Items.AddRange(@($sbAll,$sbShown,$sbInProg)) | Out-Null
    $status.Dock = 'Bottom'
    $form.Controls.Add($status)

    # Local, non-global storage for queue data
    $queue_all = New-Object System.Collections.ArrayList

    # init worker list once from config
    $workers_populated = $false
$populateWorkers = {
    if ($workers_populated) { return }
    try {
        $list = @()
        if ($cfg.PSObject.Properties['Workers'] -and $cfg.Workers) {
            $raw = @()
            if ($cfg.Workers -is [hashtable]) { $raw = @($cfg.Workers.Keys) }
            elseif ($cfg.Workers -is [System.Collections.IEnumerable]) { $raw = @($cfg.Workers) }
            else { $raw = @($cfg.Workers) }

            $expandEnv = { param([string]$v)
                if ($v -match '(?i)^\s*\$env:([A-Za-z0-9_]+)\s*$') {
                    $n = $Matches[1]
                    $val = [Environment]::GetEnvironmentVariable($n)
                    if ($val) { return $val } else { return "" }
                }
                return $v
            }
            $list = @($raw | ForEach-Object { & $expandEnv ("" + $_) } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        } else {
            # strict: use only what is in configuration
            $list = @()
        }
        $seen2 = @{}
        $ordered = @('All')
        foreach ($w in $list) { $s = "" + $w; if (-not $seen2.ContainsKey($s)) { $seen2[$s] = $true; $ordered += $s } }
        $cbWorker.Items.Clear()
        $cbWorker.Items.AddRange(@($ordered)) | Out-Null
        # prefer configured first worker if any, otherwise 'All'
        if ($list.Count -gt 0) { $cbWorker.SelectedIndex = 1 } else { $cbWorker.SelectedIndex = 0 }
        $workers_populated = $true
    } catch {
        [void][System.Windows.Forms.MessageBox]::Show($form, "Failed to populate workers: " + $_.Exception.Message, "FSLogixTools", [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Error)
    }
}

    if ($populateWorkers -is [scriptblock]) { & $populateWorkers }

    # helpers
    $fmtDate = { param([string]$iso) if ([string]::IsNullOrWhiteSpace($iso)) { "" } else { try { (Get-Date $iso).ToString("yyyy-MM-dd HH:mm:ss") } catch { "" } } }
    $makeItem = {
        param($req, $type, $state, $msg, $completedIso, $fileName)
        [pscustomobject]@{
            GroupId         = "" + $req.GroupId
            Id              = "" + $req.Id
            State           = $state
            Message         = $msg
            Operation       = "" + $req.Operation
            Worker          = "" + $req.Worker
            RequestedBy     = "" + $req.RequestedBy
            Priority        = "" + $req.Priority
            Created         = & $fmtDate $req.CreatedUtc
            Scheduled       = & $fmtDate $req.ScheduledUtc
            Completed       = & $fmtDate $completedIso
        }
    }

    $applyFilter = {
    # Preserve selection + scroll by Id
    $selectedId = $null
    $firstIndex = $null
    try {
        if ($grid.CurrentRow -and $grid.CurrentRow.DataBoundItem) {
            $selectedId = "" + $grid.CurrentRow.DataBoundItem.Id
        }
        $firstIndex = $grid.FirstDisplayedScrollingRowIndex
    } catch { }

    $filtered = $queue_all

    $stateSel = $cbState.SelectedItem
    if ($stateSel -and $stateSel -ne 'All') { $filtered = @($filtered | Where-Object { $_.State -eq $stateSel }) }

    $workerSel = $cbWorker.SelectedItem
    if ($workerSel -and $workerSel -ne 'All') {
        $filtered = @($filtered | Where-Object {
            if ($_.PSObject.Properties['WorkerList'] -and $_.WorkerList) { $_.WorkerList -contains $workerSel }
            else { $_.Worker -eq $workerSel }
        })
    }

    # --- Build a fresh DataTable and bind (robust on PS 5.1) ---
    $dt = New-Object System.Data.DataTable 'QueueView'
    foreach ($name in $script:FT_columns) { [void]$dt.Columns.Add($name, [string]) }
    foreach ($it in $filtered) {
        $row = $dt.NewRow()
        foreach ($name in $script:FT_columns) {
            try {
                $val = $null
                if ($it.PSObject.Properties[$name]) { $val = $it.$name }
                $row[$name] = "" + $val
            } catch { $row[$name] = "" }
        }
        [void]$dt.Rows.Add($row)
    }

    $grid.DataSource = $null
    $grid.DataSource = $dt

    try {
        if ($selectedId) {
            for ($i=0; $i -lt $grid.Rows.Count; $i++) {
                $row = $grid.Rows[$i]
                if ($row.DataBoundItem -and ("" + $row.Cells['Id'].Value) -eq $selectedId) {
                    $grid.CurrentCell = $row.Cells[0]
                    break
                }
            }
        }
        if ($null -ne $firstIndex -and $firstIndex -ge 0 -and $firstIndex -lt $grid.Rows.Count) {
            $grid.FirstDisplayedScrollingRowIndex = $firstIndex
        }
    } catch { }

    $total = $queue_all.Count
    $shown = $dt.Rows.Count
    $inprog = (@($filtered | Where-Object { $_.State -eq 'InProgress' })).Count
    $sbAll.Text = "Total: $total"
    $sbShown.Text = "Shown: $shown"
    $sbInProg.Text = "InProgress: $inprog"
}

    # NOTE: worker population is defined earlier via $populateWorkers.  Duplicate definition removed.

    # configurable limits: default Processed=300, Requests=300 (0 => no limit)
    $maxProcessed = 300; $maxRequests = 300
    try {
        if ($cfg.PSObject.Properties['Gui'] -and $cfg.Gui) {
            if ($cfg.Gui.PSObject.Properties['MaxProcessed']) { $maxProcessed = [int]$cfg.Gui.MaxProcessed }
            if ($cfg.Gui.PSObject.Properties['MaxRequests'])  { $maxRequests  = [int]$cfg.Gui.MaxRequests }
        }
    } catch { $maxProcessed = 300; $maxRequests = 300 }

    $loadOneDirectory = {
        param([string]$dirPath, [bool]$processed, [System.Collections.ArrayList]$items, [string]$filter='*.json')
        if (-not $dirPath -or -not (Test-Path -LiteralPath $dirPath)) { return }
        $files = Get-ChildItem -LiteralPath $dirPath -Filter $filter -File | Sort-Object LastWriteTimeUtc -Descending
        $limit = if ($processed) { $maxProcessed } else { $maxRequests }
        if ($limit -gt 0) { $files = $files | Select-Object -First $limit }
        foreach ($file in $files) {
            $statePath = [System.IO.Path]::ChangeExtension($file.FullName, '.state')
            $type='Request'; $state='Queued'; if ($processed) { $state='Completed' }; $msg=''; $completedIso=$null
            if (Test-Path -LiteralPath $statePath) {
                try {
                    $s = Get-Content -LiteralPath $statePath -Raw -Encoding UTF8 | ConvertFrom-Json
                    if ($s.Type) { $type = "" + $s.Type }
                    if ($s.State) { $state = "" + $s.State }
                    if ($s.Message) { $msg = "" + $s.Message }
                    if ($s.CompletedUtc) { $completedIso = "" + $s.CompletedUtc }
                } catch {
                    if (-not $processed) { $state = 'InProgress' }
                }
            }
            try {
                $req = Read-FTRequestFile -Path $file.FullName
            } catch {
                $req = [pscustomobject]@{ Id=""; Operation=""; Worker=""; RequestedBy=""; Priority=""; SourceComputer=""; CreatedUtc=$null; ScheduledUtc=$null }
            }
            [void]$items.Add((& $makeItem $req $type $state $msg $completedIso $file.Name))
        }
    }

    $loadQueue = {
        try {
            $items = New-Object System.Collections.ArrayList
            $paths2 = Get-FTPaths -Config $cfg
            # determine file filter for UI
            $uiFilter = '*.json'
            try {
                if ($cfg.PSObject.Properties['Gui'] -and $cfg.Gui -and $cfg.Gui.PSObject.Properties['RequestFileFilter']) { $uiFilter = "" + $cfg.Gui.RequestFileFilter }
                elseif ($cfg.PSObject.Properties['Agent'] -and $cfg.Agent -and $cfg.Agent.PSObject.Properties['RequestFileFilter']) { $uiFilter = "" + $cfg.Agent.RequestFileFilter }
            } catch { $uiFilter = '*.json' }
            & $loadOneDirectory $paths2.Requests $false $items $uiFilter
            $includeProcessed = $true
            try { if ($cfg.PSObject.Properties['Gui'] -and $cfg.Gui.PSObject.Properties['IncludeProcessed']) { $includeProcessed = [bool]$cfg.Gui.IncludeProcessed } } catch { $includeProcessed = $true }
            if ($includeProcessed) { & $loadOneDirectory $paths2.Processed $true  $items $uiFilter }
            $queue_all = $items
            & $applyFilter
        } catch {
            [System.Windows.Forms.MessageBox]::Show(
                "Failed to load queue: $($_.Exception.Message)",
                "FSLogixTools - Error",
                [System.Windows.Forms.MessageBoxButtons]::OK,
                [System.Windows.Forms.MessageBoxIcon]::Error
            ) | Out-Null
        }
    }

    $btnRefresh.Add_Click({ if ($populateWorkers -is [scriptblock]) { & $populateWorkers }
    try { $timer.Stop() } catch { } ; & $loadQueue ; try { if ($cbAuto.SelectedItem -and ($cbAuto.SelectedItem -ne 'Off')) { $timer.Start() } } catch { } })
    $cbState.Add_SelectedIndexChanged({ & $applyFilter })
    $cbWorker.Add_SelectedIndexChanged({ & $applyFilter })
    $btnNew.Add_Click({ Show-NewRequestDialog -Owner $form -cfg $cfg; & $loadQueue })

    # perform an initial queue load before displaying the form
    & $loadQueue
    # stop timer when window is closing
    $form.Add_FormClosing({ try { $timer.Stop() } catch { } })
    [void]$form.ShowDialog()
}

#endregion GUI

Export-ModuleMember -Function Start-FTAgent, Start-FTGui
