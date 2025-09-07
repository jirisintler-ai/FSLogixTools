# FSLogixTools.psm1
# Version 0.9.2
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
                $json = Get-Content -LiteralPath $p -Raw -ErrorAction Stop
                $cfg  = $json | ConvertFrom-Json -ErrorAction Stop
                Add-Member -InputObject $cfg -NotePropertyName SourcePath -NotePropertyValue $p -Force
                return $cfg
            } catch {
                throw "Failed to read/parse configuration file '$p': $($_.Exception.Message)"
            }
        }
    }

    $list = ($candidates | Select-Object -Unique) -join "`r`n  - "
    throw ("Configuration file not found. Looked in:`r`n  - {0}" -f $list)
}

function New-FTDirectory {
    param([Parameter(Mandatory=$true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}
function Ensure-FTStructure {
    param([object]$Config)
    try {
        if (-not $Config) { return }
        $paths = @()
        if ($Config.Paths -and $Config.Paths.Root) { $paths += @($Config.Paths.Root) }
        foreach ($k in @('Requests','Processed','Logs')) {
            if ($Config.Paths -and $Config.Paths.$k) { $paths += @($Config.Paths.$k) }
        }
        if ($Config.Paths -and $Config.Paths.Root) {
            $paths += (Join-Path $Config.Paths.Root 'Templates')
        }
        foreach ($p in $paths) {
            if ($p) { New-FTDirectory -Path $p }
        }
    } catch { }
}


function Write-FTLog {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)][string] $Message,
        [ValidateSet('Trace','Info','Warn','Error')][string] $Level = 'Info',
        [object] $Config
    )
    $cfg = if ($Config) { $Config } else { Get-FTConfig }
    # honor log level filtering when a config is provided
    $shouldWrite = $true
    try {
        # define severity order from least to most severe
        $levelMap = @{ 'Trace' = 0; 'Info' = 1; 'Warn' = 2; 'Error' = 3 }
        if ($cfg.PSObject.Properties['Agent'] -and $cfg.Agent -and $cfg.Agent.PSObject.Properties['LogLevel']) {
            $minName = "" + $cfg.Agent.LogLevel
            if ($levelMap.ContainsKey($minName) -and $levelMap.ContainsKey($Level)) {
                $minVal = $levelMap[$minName]
                $curVal = $levelMap[$Level]
                if ($curVal -lt $minVal) { $shouldWrite = $false }
            }
        }
    } catch { $shouldWrite = $true }
    if (-not $shouldWrite) { return }

    $logsRoot = $cfg.Paths.Logs
    New-FTDirectory -Path $logsRoot

    $date = Get-Date
    $file = Join-Path $logsRoot ("{0}_{1:yyyyMMdd}.log" -f $env:COMPUTERNAME, $date)
    $line = "[{0:yyyy-MM-dd HH:mm:ss.fff}] [{1}] {2}" -f $date, $Level.ToUpper(), $Message

    $enc = New-Object System.Text.UTF8Encoding($true)  # UTF-8 BOM
    [System.IO.File]::AppendAllText($file, $line + [Environment]::NewLine, $enc)
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
    $enc = New-Object System.Text.UTF8Encoding($true)
    [System.IO.File]::WriteAllText($Path, $json, $enc)
}

function Read-FTRequests {
    param([Parameter(Mandatory=$true)][string] $RequestsPath)
    if (-not (Test-Path -LiteralPath $RequestsPath)) { return @() }
    Get-ChildItem -LiteralPath $RequestsPath -Filter *.json -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTimeUtc
}

function Read-FTRequestFile {
    param([Parameter(Mandatory=$true)][string]$Path)
    $raw = Get-Content -LiteralPath $Path -Raw -ErrorAction Stop
    return $raw | ConvertFrom-Json -ErrorAction Stop
}

function Complete-FTRequest {
    param(
        [Parameter(Mandatory=$true)][string] $RequestPath,
        [Parameter(Mandatory=$true)][string] $ProcessedDir,
        [string] $StatePath
    )
    New-FTDirectory -Path $ProcessedDir
    $name = Split-Path -Leaf $RequestPath
    $stamp = Get-Date -Format 'yyyyMMdd_HHmmssfff'
    $dest  = Join-Path $ProcessedDir ($stamp + '_' + $name)
    Move-Item -LiteralPath $RequestPath -Destination $dest -Force
    if ($StatePath -and (Test-Path -LiteralPath $StatePath)) {
        $stateDest = [System.IO.Path]::ChangeExtension($dest, '.state')
        Move-Item -LiteralPath $StatePath -Destination $stateDest -Force
    }
    return $dest
}

#endregion Helpers

#region Agent

function Start-FTAgent {
    [CmdletBinding()]
    param(
        [Parameter(Position=0)]
        [Alias('Config','Path')]
        [string] $ConfigPath,
        [Alias('AsWorker')]
        [string] $Worker,
        [switch] $Once
    )

    if (-not $ConfigPath -and $args.Count -eq 1 -and -not $Once) { $ConfigPath = $args[0] }

    $cfg = Get-FTConfig -ConfigPath $ConfigPath

    Ensure-FTStructure -Config $cfg
    $thisWorker = if ($Worker -and -not [string]::IsNullOrWhiteSpace($Worker)) { "" + $Worker } else { $env:COMPUTERNAME }
    Write-Verbose ("Agent worker: {0}" -f $thisWorker)
    $poll = [int]$cfg.Agent.PollSeconds
    if ($poll -lt 1) { $poll = 10 }
    $maxConcurrent = [int]$cfg.Agent.MaxConcurrent
    if ($maxConcurrent -lt 1) { $maxConcurrent = 1 }

    $stopOnError = $false
    try {
        if ($cfg.PSObject.Properties['Agent'] -and $cfg.Agent.PSObject.Properties['StopOnError']) {
            $stopOnError = [bool]$cfg.Agent.StopOnError
        }
    } catch { $stopOnError = $false }


    Write-FTLog -Message "FTAgent starting. Computer=$thisWorker, MaxConcurrent=$maxConcurrent, Poll=$poll s" -Level Info -Config $cfg

    do {
        try {
            $reqDir  = $cfg.Paths.Requests
            $procDir = $cfg.Paths.Processed

            New-FTDirectory -Path $reqDir
            New-FTDirectory -Path $procDir

            $files = Read-FTRequests -RequestsPath $reqDir

            $toProcess = @()
            foreach ($f in $files) {
                try {
                    $req = Read-FTRequestFile -Path $f.FullName
                    if ($req.Worker -and $req.Worker -ne $thisWorker) { continue }
                    $toProcess += [pscustomobject]@{ File=$f; Data=$req }
                } catch { }
            }

            $count = 0
            $fatal = $false
            foreach ($item in $toProcess) {
                if ($count -ge $maxConcurrent) { break }
                $count++

                $req = $item.Data
                $filePath = $item.File.FullName
                $reqId = if ($req.Id) { $req.Id } else { [guid]::NewGuid().Guid }
                $statePath = [System.IO.Path]::ChangeExtension($filePath, '.state')
                $typeValue = if ($null -ne $req -and $req.PSObject.Properties['Type'] -and $req.Type) { $req.Type } else { 'Request' }

                try {
                    Write-FTState -Path $statePath -State InProgress -Type $typeValue -Percent 1 -Message "Starting" -Worker $thisWorker -RequestId $reqId

                    switch -Regex ($req.Operation) {
                        '^Test$'    {
                            Write-FTState -Path $statePath -State InProgress -Type $typeValue -Percent 25 -Message "Step 1/3" -Worker $thisWorker -RequestId $reqId
                            Start-Sleep -Milliseconds 180
                            Write-FTState -Path $statePath -State InProgress -Type $typeValue -Percent 66 -Message "Step 2/3" -Worker $thisWorker -RequestId $reqId
                            Start-Sleep -Milliseconds 180
                            Write-FTState -Path $statePath -State InProgress -Type $typeValue -Percent 85 -Message "Step 3/3" -Worker $thisWorker -RequestId $reqId
                        }
                        '^Cleanup$' {
                            Write-FTState -Path $statePath -State InProgress -Type $typeValue -Percent 50 -Message "Cleaning" -Worker $thisWorker -RequestId $reqId
                            Start-Sleep -Milliseconds 300
                        }
                        default     {
                            Start-Sleep -Milliseconds 150
                        }
                    }

                    Write-FTState -Path $statePath -State Completed -Type $typeValue -Percent 100 -Message "Done" -Worker $thisWorker -RequestId $reqId -CompletedUtc ([DateTime]::UtcNow.ToString('o'))
                    $null = Complete-FTRequest -RequestPath $filePath -ProcessedDir $procDir -StatePath $statePath
                } catch {
                    Write-FTState -Path $statePath -State Failed -Type $typeValue -Percent 0 -Message $_.Exception.Message -Worker $thisWorker -RequestId $reqId -CompletedUtc ([DateTime]::UtcNow.ToString('o'))
                    if ($stopOnError) { $fatal = $true; Write-FTLog -Message "Stopping agent due to error and StopOnError=true" -Level Error -Config $cfg }
                }
            }
        if ($fatal) { break }
        } catch { }

        if ($Once) { break }
        Start-Sleep -Seconds $poll
    } while ($true)

    Write-FTLog -Message "FTAgent stopped." -Level Info -Config $cfg
}

#endregion Agent

#region GUI (WinForms)

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
    if (-not $Browse) {
        try { $cfg = Get-FTConfig -ConfigPath $ConfigPath } catch { $cfg = $null }

    Ensure-FTStructure -Config $cfg
    }
    if (-not $cfg) {
        # show a simple dialog when configuration cannot be loaded
        Add-Type -AssemblyName System.Windows.Forms
        [System.Windows.Forms.MessageBox]::Show(
            "Configuration file not found or failed to load.",
            "FSLogixTools",
            [System.Windows.Forms.MessageBoxButtons]::OK,
            [System.Windows.Forms.MessageBoxIcon]::Error
        ) | Out-Null
        return
    }

    $form              = New-Object System.Windows.Forms.Form
    $form.Text         = "FSLogixTools"
    $form.StartPosition= "CenterScreen"
    $form.Size         = New-Object System.Drawing.Size(1260, 680)
    $form.TopMost      = $false

    $btnRefresh = New-Object System.Windows.Forms.Button
    $btnRefresh.Text = "Refresh"
    $btnRefresh.Location = New-Object System.Drawing.Point(10, 10)
    $btnRefresh.Size = New-Object System.Drawing.Size(90, 28)
    $form.Controls.Add($btnRefresh)

    $btnRunOnce = New-Object System.Windows.Forms.Button
    $btnRunOnce.Text = "Run Agent Once"
    $btnRunOnce.Location = New-Object System.Drawing.Point(110, 10)
    $btnRunOnce.Size = New-Object System.Drawing.Size(140, 28)
    $form.Controls.Add($btnRunOnce)

    $btnNew = New-Object System.Windows.Forms.Button
    $btnNew.Text = "New Request"
    $btnNew.Location = New-Object System.Drawing.Point(260, 10)
    $btnNew.Size = New-Object System.Drawing.Size(120, 28)
    $form.Controls.Add($btnNew)

    $lblState = New-Object System.Windows.Forms.Label
    $lblState.Text = "State:"
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

    $lblWorker = New-Object System.Windows.Forms.Label
    $lblWorker.Text = "Worker:"
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

    
# Auto refresh controls
$lblAuto = New-Object System.Windows.Forms.Label
$lblAuto.Text = "Auto:"; $lblAuto.AutoSize = $true
$lblAuto.Location = New-Object System.Drawing.Point(820, 16)
$lblAuto.Anchor = [System.Windows.Forms.AnchorStyles]::Top -bor [System.Windows.Forms.AnchorStyles]::Left
$form.Controls.Add($lblAuto)

$cbAuto = New-Object System.Windows.Forms.ComboBox
$cbAuto.DropDownStyle = 'DropDownList'
$cbAuto.Location = New-Object System.Drawing.Point(870, 12)
$cbAuto.Size = New-Object System.Drawing.Size(90, 24)
$cbAuto.Anchor = [System.Windows.Forms.AnchorStyles]::Top -bor [System.Windows.Forms.AnchorStyles]::Left
$cbAuto.Items.AddRange(@('Off','5 s','10 s','30 s','60 s')) | Out-Null
$cbAuto.SelectedIndex = 0
$form.Controls.Add($cbAuto)

$timer = New-Object System.Windows.Forms.Timer
$timer.Interval = 10000
$timer.Add_Tick({ & $loadQueue })

    # Default auto refresh value from configuration
    try {
        $defaultSecs = 0
        if ($cfg.PSObject.Properties['Gui'] -and $cfg.Gui) {
            # Prefer explicit AutoRefreshSeconds if present
            if ($cfg.Gui.PSObject.Properties['AutoRefreshSeconds']) {
                $defaultSecs = [int]$cfg.Gui.AutoRefreshSeconds
            } elseif ($cfg.Gui.PSObject.Properties['AutoRefreshSecondsDefault']) {
                # fall back to AutoRefreshSecondsDefault if defined
                $defaultSecs = [int]$cfg.Gui.AutoRefreshSecondsDefault
            } elseif ($cfg.Gui.PSObject.Properties['AutoRefresh']) {
                # support legacy string value such as '5 s' or 'Off'
                switch ("" + $cfg.Gui.AutoRefresh) {
                    'Off' { $defaultSecs = 0 }
                    '5 s' { $defaultSecs = 5 }
                    '10 s' { $defaultSecs = 10 }
                    '30 s' { $defaultSecs = 30 }
                    '60 s' { $defaultSecs = 60 }
                    default { $defaultSecs = 0 }
                }
            }
        }
        # select the appropriate item in the Auto dropdown
        switch ($defaultSecs) {
            5   { $cbAuto.SelectedItem = '5 s' }
            10  { $cbAuto.SelectedItem = '10 s' }
            30  { $cbAuto.SelectedItem = '30 s' }
            60  { $cbAuto.SelectedItem = '60 s' }
            default { $cbAuto.SelectedItem = 'Off' }
        }
        # configure and start/stop the timer based on the selected value
        switch ($cbAuto.SelectedItem) {
            'Off'  { $timer.Stop() }
            '5 s'  { $timer.Interval = 5000;  $timer.Start() }
            '10 s' { $timer.Interval = 10000; $timer.Start() }
            '30 s' { $timer.Interval = 30000; $timer.Start() }
            '60 s' { $timer.Interval = 60000; $timer.Start() }
            default { $timer.Stop() }
        }
    } catch { }

$cbAuto.Add_SelectedIndexChanged({
    switch ($cbAuto.SelectedItem) {
        'Off'  { $timer.Stop() }
        '5 s'  { $timer.Interval = 5000;  $timer.Start() }
        '10 s' { $timer.Interval = 10000; $timer.Start() }
        '30 s' { $timer.Interval = 30000; $timer.Start() }
        '60 s' { $timer.Interval = 60000; $timer.Start() }
        default { $timer.Stop() }
    }
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

    foreach ($col in @(
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
    )) {
        $c = New-Object System.Windows.Forms.DataGridViewTextBoxColumn
        $c.Name = $col.Name
        $c.HeaderText = $col.Header
        $c.DataPropertyName = $col.Name
        $c.Width = $col.Width
        $grid.Columns.Add($c) | Out-Null
    }
$script:FT_columns = @('GroupId','Id','State','Message','Operation','Worker','Priority','RequestedBy','Created','Scheduled','Completed')

    $form.Controls.Add($grid)

    $status = New-Object System.Windows.Forms.StatusStrip
    $sbAll = New-Object System.Windows.Forms.ToolStripStatusLabel
    $sbShown = New-Object System.Windows.Forms.ToolStripStatusLabel
    $sbInProg = New-Object System.Windows.Forms.ToolStripStatusLabel
    $status.Items.AddRange(@($sbAll,$sbShown,$sbInProg)) | Out-Null
    $status.Dock = 'Bottom'
    $form.Controls.Add($status)

    $global:FT_queue_all = New-Object System.Collections.ArrayList

    # init worker list once from config
    $global:FT_workers_populated = $false
$populateWorkers = {
    if ($global:FT_workers_populated) { return }
    try {
        $list = @()
        if ($cfg.PSObject.Properties['Workers'] -and $cfg.Workers -and $cfg.Workers.Count -gt 0) {
            $expandEnv = { param([string]$v)
                if ($v -match '^\s*\$env:([A-Za-z0-9_]+)\s*$') {
                    $n = $Matches[1]
                    $val = [Environment]::GetEnvironmentVariable($n)
                    if ($val) { return $val } else { return "" }
                }
                return $v
            }
            $list = @($cfg.Workers | ForEach-Object { & $expandEnv ("" + $_) } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        } else {
            $list = @($env:COMPUTERNAME)
        }
        $distinct = @('All') + ($list | Sort-Object -Unique)
        $cbWorker.Items.Clear()
        $cbWorker.Items.AddRange(@($distinct)) | Out-Null
        $idx = [Array]::IndexOf($cbWorker.Items, $env:COMPUTERNAME)
        if ($idx -ge 0) { $cbWorker.SelectedIndex = $idx } else { $cbWorker.SelectedIndex = 0 }
        $global:FT_workers_populated = $true
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

    $filtered = $global:FT_queue_all

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
        if ($firstIndex -ne $null -and $firstIndex -ge 0 -and $firstIndex -lt $grid.Rows.Count) {
            $grid.FirstDisplayedScrollingRowIndex = $firstIndex
        }
    } catch { }

    $total = $global:FT_queue_all.Count
    $shown = $dt.Rows.Count
    $inprog = (@($filtered | Where-Object { $_.State -eq 'InProgress' })).Count
    $sbAll.Text = "Total: $total"
    $sbShown.Text = "Shown: $shown"
    $sbInProg.Text = "InProgress: $inprog"
}

    # NOTE: worker population is defined earlier via $populateWorkers.  Duplicate definition removed.

    $loadOneDirectory = {
        param([string]$dirPath, [bool]$processed, [System.Collections.ArrayList]$items)
        if (-not $dirPath -or -not (Test-Path -LiteralPath $dirPath)) { return }
        $files = Get-ChildItem -LiteralPath $dirPath -Filter *.json -File | Sort-Object LastWriteTimeUtc -Descending
        if ($processed) { $files = $files | Select-Object -First 300 }
        foreach ($file in $files) {
            $statePath = [System.IO.Path]::ChangeExtension($file.FullName, '.state')
            $type='Request'; $state='Queued'; if ($processed) { $state='Completed' }; $msg=''; $completedIso=$null
            if (Test-Path -LiteralPath $statePath) {
                try {
                    $s = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
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
            & $loadOneDirectory $cfg.Paths.Requests $false $items
            & $loadOneDirectory $cfg.Paths.Processed $true  $items
            $global:FT_queue_all = $items
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
    & $loadQueue })
    $cbState.Add_SelectedIndexChanged({ & $applyFilter })
    $cbWorker.Add_SelectedIndexChanged({ & $applyFilter })
    $btnRunOnce.Add_Click({
    $w = $null
    try {
        if ($cbWorker.SelectedItem -and ("" + $cbWorker.SelectedItem) -ne 'All') { $w = "" + $cbWorker.SelectedItem }
    } catch { }
    if ($w) { Start-FTAgent -ConfigPath $cfg.SourcePath -Once -Worker $w } else { Start-FTAgent -ConfigPath $cfg.SourcePath -Once }
    & $loadQueue
})
    
function Show-NewRequestDialog {
    param($Parent,$cfg)

        $dlg = New-Object System.Windows.Forms.Form
        $dlg.Text = "New Request"
        $dlg.StartPosition = "CenterParent"
        $dlg.Size = New-Object System.Drawing.Size(620, 520)
        $dlg.MaximizeBox = $false
        $dlg.MinimizeBox = $false
        $dlg.FormBorderStyle = 'FixedDialog'

        function New-Label([string]$text, [int]$x, [int]$y) { $l = New-Object System.Windows.Forms.Label; $l.Text = $text; $l.Location = New-Object System.Drawing.Point($x,$y); $l.AutoSize = $true; return $l }
        function New-Text([int]$x,[int]$y,[int]$w) { $t = New-Object System.Windows.Forms.TextBox; $t.Location = New-Object System.Drawing.Point($x,$y); $t.Size = New-Object System.Drawing.Size($w, 22); return $t }

        # Request Type
        $dlg.Controls.Add((New-Label "Request Type*" 20 20))
        $cmbType = New-Object System.Windows.Forms.ComboBox
        $cmbType.DropDownStyle = 'DropDownList'
        $cmbType.Location = New-Object System.Drawing.Point(160, 18)
        $cmbType.Size = New-Object System.Drawing.Size(420, 24)
        $cmbType.Items.Clear()
        $cmbType.Items.AddRange(@("Template")) | Out-Null
        $cmbType.SelectedIndex = 0
        $dlg.Controls.Add($cmbType)


        # Template
        $dlg.Controls.Add((New-Label "Template*" 20 56))
        $cmbTemplate = New-Object System.Windows.Forms.ComboBox
        $cmbTemplate.DropDownStyle = 'DropDownList'
        $cmbTemplate.Location = New-Object System.Drawing.Point(160, 54)
        $cmbTemplate.Size = New-Object System.Drawing.Size(420, 24)
        try {
            $templatesDir = Join-Path $cfg.Paths.Root 'Templates'
            if (Test-Path -LiteralPath $templatesDir) {
                $tmpl = Get-ChildItem -LiteralPath $templatesDir -Filter '*.request.json' -File -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Name
                if ($tmpl) { $cmbTemplate.Items.AddRange(@($tmpl)) | Out-Null }
            }
        } catch { }
        if ($cmbTemplate.Items.Count -gt 0) { $cmbTemplate.SelectedIndex = 0
        # ##IMMEDIATE_RENDER_INSERTED
        try {
            $sel = "" + $cmbTemplate.SelectedItem
            if (-not [string]::IsNullOrWhiteSpace($sel)) {
                $templateFile = Join-Path (Join-Path $cfg.Paths.Root 'Templates') $sel
                if (Test-Path -LiteralPath $templateFile) {
                    $tmplRaw = Get-Content -LiteralPath $templateFile -Raw -Encoding UTF8
                    $tmplObj = $tmplRaw | ConvertFrom-Json -ErrorAction Stop
                    if ($pnlParams) { & $renderParams $tmplObj }  # ##IMMEDIATE_RENDER_GUARDED
                }
            }
        } catch {
            $lbl = New-Object System.Windows.Forms.Label
            $lbl.Text = "Template render failed: $($_.Exception.Message)"
            $lbl.ForeColor = [System.Drawing.Color]::DarkRed
            $lbl.AutoSize = $true
            if ($pnlParams) { $pnlParams.Controls.Clear(); [void]$pnlParams.Controls.Add($lbl) }
        } }
        $dlg.Controls.Add($cmbTemplate)
        # Ensure initial render after form shows
        )

        $cmbTemplate.Add_SelectedIndexChanged({
            try {
                $sel = "" + $cmbTemplate.SelectedItem
                if ([string]::IsNullOrWhiteSpace($sel)) { return }
                $templateFile = Join-Path (Join-Path $cfg.Paths.Root 'Templates') $sel
                if (-not (Test-Path -LiteralPath $templateFile)) { return }
                $tmplRaw = Get-Content -LiteralPath $templateFile -Raw -Encoding UTF8
                $tmplObj = $tmplRaw | ConvertFrom-Json -ErrorAction Stop
                if ($pnlParams) { & $renderParams $tmplObj }  # ##IMMEDIATE_RENDER_GUARDED
            } catch { }
        })


        # Workers
        $dlg.Controls.Add((New-Label "Worker(s)" 20 56))
        $chkAll = New-Object System.Windows.Forms.CheckBox
        $chkAll.Text = "All"
        $chkAll.Location = New-Object System.Drawing.Point(160, 54)
        $chkAll.AutoSize = $true
        $dlg.Controls.Add($chkAll)

        $clbWorkers = New-Object System.Windows.Forms.CheckedListBox
        $clbWorkers.Location = New-Object System.Drawing.Point(160, 80)
        $clbWorkers.Size = New-Object System.Drawing.Size(420, 90)
        $clbWorkers.CheckOnClick = $true
        $wlist = @()
if ($cfg.PSObject.Properties['Workers'] -and $cfg.Workers -and $cfg.Workers.Count -gt 0) {
    $wlist = @($cfg.Workers)
} else {
    $wlist = @($env:COMPUTERNAME)
}
# Expand $env:NAME tokens and de-duplicate
$wexp = @()
foreach ($w in $wlist) {
    $t = "" + $w
    if ($t -match '^\s*\$env:([A-Za-z0-9_]+)\s*$') {
        $name = $Matches[1]
        $val = [Environment]::GetEnvironmentVariable($name)
        if ($val) { $wexp += $val }
    } elseif ($t) {
        $wexp += $t
    }
}
$wexp = @($wexp | Sort-Object -Unique)
$clbWorkers.Items.Clear()
$clbWorkers.Items.AddRange(@($wexp)) | Out-Null
        # default = current computer
        for ($i=0; $i -lt $clbWorkers.Items.Count; $i++) {
            if (("" + $clbWorkers.Items[$i]) -eq $env:COMPUTERNAME) { $clbWorkers.SetItemChecked($i, $true) }
        }
        $dlg.Controls.Add($clbWorkers)

        $chkAll.Add_CheckedChanged({
            for ($i=0; $i -lt $clbWorkers.Items.Count; $i++) {
                $clbWorkers.SetItemChecked($i, $chkAll.Checked)
            }
        })

        # Parameters (dynamic from Template)
        # Notes
        $dlg.Controls.Add((New-Label "Notes" 20 332))
        $txtNotes = New-Text 160 330 420
        $dlg.Controls.Add($txtNotes)
        # Parameters (dynamic UI)
        $dlg.Controls.Add((New-Label "Parameters" 20 290))
        $pnlParams = New-Object System.Windows.Forms.Panel
        $pnlParams.Location = New-Object System.Drawing.Point(160, 288)
        $pnlParams.Size     = New-Object System.Drawing.Size(420, 160)
        $pnlParams.AutoScroll = $true
        $dlg.Controls.Add($pnlParams)
        # Initial render once the dialog is shown (panel exists)
        $dlg.Add_Shown({
            try {
                if ($cmbTemplate.Items.Count -gt 0 -and -not $cmbTemplate.SelectedItem) { $cmbTemplate.SelectedIndex = 0 }
                $sel = "" + $cmbTemplate.SelectedItem
                if ([string]::IsNullOrWhiteSpace($sel)) { return }
                $templateFile = Join-Path (Join-Path $cfg.Paths.Root 'Templates') $sel
                if (-not (Test-Path -LiteralPath $templateFile)) { return }
                $tmplRaw = Get-Content -LiteralPath $templateFile -Raw -Encoding UTF8
                $tmplObj = $tmplRaw | ConvertFrom-Json -ErrorAction Stop
                & $renderParams $tmplObj
            } catch { }
        })


        # storage for generated parameter controls
            $script:paramControls = @{}
        # helper to render parameter controls from a template object
        $renderParams = {
            param([object] $tmplObjIn)
            if (-not $pnlParams) { return }
            try {
                $pnlParams.SuspendLayout()
            } catch {}
            try {
                $pnlParams.Controls.Clear()
                $script:paramControls = @{}
                $y = 0

                if ($tmplObjIn -and $tmplObjIn.PSObject.Properties['ParameterDefs'] -and $tmplObjIn.ParameterDefs) {
                    foreach ($def in $tmplObjIn.ParameterDefs) {
                        $name  = "" + $def.Name
                        if ([string]::IsNullOrWhiteSpace($name)) { continue }
                        $type  = if ($def.PSObject.Properties['Type']) { ("" + $def.Type).ToLowerInvariant() } else { 'string' }
                        $label = if ($def.PSObject.Properties['Label'] -and $def.Label) { "" + $def.Label } else { $name }
                        $dflt  = $null
                        if ($def.PSObject.Properties['Default']) { $dflt = $def.Default }
                        elseif ($tmplObjIn.PSObject.Properties['Parameters'] -and $tmplObjIn.Parameters -and $tmplObjIn.Parameters.PSObject.Properties[$name]) { $dflt = $tmplObjIn.Parameters.$name }

                        $lbl = New-Object System.Windows.Forms.Label
                        $lbl.Text = $label
                        $lbl.Location = New-Object System.Drawing.Point(0, $y+4)
                        $lbl.AutoSize = $true
                        [void]$pnlParams.Controls.Add($lbl)

                        switch ($type) {
                            'bool' {
                                $ctl = New-Object System.Windows.Forms.CheckBox
                                $ctl.Location = New-Object System.Drawing.Point(200, $y)
                                $ctl.AutoSize = $true
                                if ($dflt -ne $null) { $ctl.Checked = [bool]$dflt }
                                [void]$pnlParams.Controls.Add($ctl)
                                $script:paramControls[$name] = @{ Type='bool'; Control=$ctl }
                                $y += 28
                            }
                            'int' {
                                $ctl = New-Object System.Windows.Forms.NumericUpDown
                                $ctl.Location = New-Object System.Drawing.Point(200, $y)
                                $ctl.Size     = New-Object System.Drawing.Size(200, 24)
                                $ctl.Minimum  = -2147483648
                                $ctl.Maximum  =  2147483647
                                if ($def.PSObject.Properties['Min']) { try { $ctl.Minimum = [decimal]$def.Min } catch {} }
                                if ($def.PSObject.Properties['Max']) { try { $ctl.Maximum = [decimal]$def.Max } catch {} }
                                if ($dflt -ne $null) { try { $ctl.Value = [decimal]$dflt } catch {} }
                                [void]$pnlParams.Controls.Add($ctl)
                                $script:paramControls[$name] = @{ Type='int'; Control=$ctl }
                                $y += 28
                            }
                            'number' {
                                $ctl = New-Object System.Windows.Forms.NumericUpDown
                                $ctl.Location = New-Object System.Drawing.Point(200, $y)
                                $ctl.Size     = New-Object System.Drawing.Size(200, 24)
                                $ctl.DecimalPlaces = 2
                                $ctl.Minimum  = -1000000000
                                $ctl.Maximum  =  1000000000
                                if ($def.PSObject.Properties['Min']) { try { $ctl.Minimum = [decimal]$def.Min } catch {} }
                                if ($def.PSObject.Properties['Max']) { try { $ctl.Maximum = [decimal]$def.Max } catch {} }
                                if ($dflt -ne $null) { try { $ctl.Value = [decimal]$dflt } catch {} }
                                [void]$pnlParams.Controls.Add($ctl)
                                $script:paramControls[$name] = @{ Type='number'; Control=$ctl }
                                $y += 28
                            }
                            'select' {
                                $ctl = New-Object System.Windows.Forms.ComboBox
                                $ctl.DropDownStyle = 'DropDownList'
                                $ctl.Location = New-Object System.Drawing.Point(200, $y)
                                $ctl.Size     = New-Object System.Drawing.Size(200, 24)
                                $choices = @()
                                if ($def.PSObject.Properties['Choices'] -and $def.Choices) { $choices = @($def.Choices) }
                                if ($choices.Count -gt 0) { $ctl.Items.AddRange(@($choices)) | Out-Null }
                                if ($dflt -ne $null) {
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
                                $ctl.Location = New-Object System.Drawing.Point(200, $y)
                                $ctl.Size     = New-Object System.Drawing.Size(200, 60)
                                $ctl.CheckOnClick = $true
                                $choices = @()
                                if ($def.PSObject.Properties['Choices'] -and $def.Choices) { $choices = @($def.Choices) }
                                if ($choices.Count -gt 0) { $ctl.Items.AddRange(@($choices)) | Out-Null }
                                if ($dflt -ne $null) {
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
                                $ctl.Location = New-Object System.Drawing.Point(200, $y)
                                $ctl.Size     = New-Object System.Drawing.Size(200, 24)
                                $ctl.Format   = [System.Windows.Forms.DateTimePickerFormat]::Custom
                                $ctl.CustomFormat = "yyyy-MM-dd HH:mm:ss"
                                if ($dflt -ne $null) { try { $ctl.Value = [DateTime]$dflt } catch {} }
                                [void]$pnlParams.Controls.Add($ctl)
                                $script:paramControls[$name] = @{ Type='datetime'; Control=$ctl }
                                $y += 28
                            }
                            default {
                                $ctl = New-Object System.Windows.Forms.TextBox
                                $ctl.Location = New-Object System.Drawing.Point(200, $y)
                                $ctl.Size     = New-Object System.Drawing.Size(200, 24)
                                if ($dflt -ne $null) { $ctl.Text = "" + $dflt }
                                [void]$pnlParams.Controls.Add($ctl)
                                $script:paramControls[$name] = @{ Type='string'; Control=$ctl }
                                $y += 28
                            }
                        }
                    }
                } elseif ($tmplObjIn -and $tmplObjIn.PSObject.Properties['Parameters'] -and $tmplObjIn.Parameters) {
                    foreach ($prop in $tmplObjIn.Parameters.PSObject.Properties) {
                        $name = "" + $prop.Name
                        $lbl = New-Object System.Windows.Forms.Label
                        $lbl.Text = $name
                        $lbl.Location = New-Object System.Drawing.Point(0, $y+4)
                        $lbl.AutoSize = $true
                        [void]$pnlParams.Controls.Add($lbl)

                        $ctl = New-Object System.Windows.Forms.TextBox
                        $ctl.Location = New-Object System.Drawing.Point(200, $y)
                        $ctl.Size     = New-Object System.Drawing.Size(200, 24)
                        $ctl.Text     = "" + $prop.Value
                        [void]$pnlParams.Controls.Add($ctl)

                        $script:paramControls[$name] = @{ Type='string'; Control=$ctl }
                        $y += 28
                    }
                } else {
                    $lbl = New-Object System.Windows.Forms.Label
                    $lbl.Text = "No parameters defined in template."
                    $lbl.Location = New-Object System.Drawing.Point(0, 0)
                    $lbl.AutoSize = $true
                    [void]$pnlParams.Controls.Add($lbl)
                }
            } finally {
                try { $pnlParams.ResumeLayout() } catch {}
            }
        }
        # initial params render
        try {
            $sel = "" + $cmbTemplate.SelectedItem
            if (-not [string]::IsNullOrWhiteSpace($sel)) {
                $templateFile = Join-Path (Join-Path $cfg.Paths.Root 'Templates') $sel
                if (Test-Path -LiteralPath $templateFile) {
                    $tmplRaw = Get-Content -LiteralPath $templateFile -Raw -Encoding UTF8
                    $tmplObj = $tmplRaw | ConvertFrom-Json -ErrorAction Stop
                    if ($pnlParams) { & $renderParams $tmplObj }  # ##IMMEDIATE_RENDER_GUARDED
                }
            }
        } catch { }



        # Priority
        $dlg.Controls.Add((New-Label "Priority" 420 368))
        $cmbPriority = New-Object System.Windows.Forms.ComboBox
        $cmbPriority.DropDownStyle = 'DropDownList'
        $cmbPriority.Location = New-Object System.Drawing.Point(480, 366)
        $cmbPriority.Size = New-Object System.Drawing.Size(100, 24)
        $cmbPriority.Items.AddRange(@('Low','Normal','High')) | Out-Null
        $cmbPriority.SelectedIndex = 1
        $dlg.Controls.Add($cmbPriority)

        # Scheduled
        $dlg.Controls.Add((New-Label "Scheduled (UTC)" 20 368))
        $dtp = New-Object System.Windows.Forms.DateTimePicker
        $dtp.Location = New-Object System.Drawing.Point(160, 366)
        $dtp.Size = New-Object System.Drawing.Size(240, 24)
        $dtp.Format = [System.Windows.Forms.DateTimePickerFormat]::Custom
        $dtp.CustomFormat = "yyyy-MM-dd HH:mm:ss"
        $dtp.ShowCheckBox = $true
        $dtp.Checked = $false
        $dtp.Value = [DateTime]::UtcNow
        $dlg.Controls.Add($dtp)

        $btnOK = New-Object System.Windows.Forms.Button; $btnOK.Text = "Create"; $btnOK.Location = New-Object System.Drawing.Point(400, 420); $btnOK.Size = New-Object System.Drawing.Size(80, 28); $dlg.Controls.Add($btnOK)
        $btnCancel = New-Object System.Windows.Forms.Button; $btnCancel.Text = "Cancel"; $btnCancel.Location = New-Object System.Drawing.Point(500, 420); $btnCancel.Size = New-Object System.Drawing.Size(80, 28); $dlg.Controls.Add($btnCancel)

        $btnCancel.Add_Click({ $dlg.DialogResult = [System.Windows.Forms.DialogResult]::Cancel; $dlg.Close() })
        $btnOK.Add_Click({
            $groupId = [guid]::NewGuid().Guid
            try {
                $reqType = "" + $cmbType.SelectedItem
                if ([string]::IsNullOrWhiteSpace($reqType)) { throw "Request Type is required." }

                $template = "" + $cmbTemplate.SelectedItem
                if ([string]::IsNullOrWhiteSpace($template)) { throw "Template is required when Request Type is 'Template'." }
                $templateFile = Join-Path (Join-Path $cfg.Paths.Root 'Templates') $template
                if (-not (Test-Path -LiteralPath $templateFile)) { throw "Template file not found: $templateFile" }
                $tmplRaw = Get-Content -LiteralPath $templateFile -Raw -Encoding UTF8
                $tmplObj = $tmplRaw | ConvertFrom-Json -ErrorAction Stop

                $operation = if ($tmplObj.PSObject.Properties['Operation'] -and $tmplObj.Operation) { "" + $tmplObj.Operation } else { $reqType }
                $notes     = if ($tmplObj.PSObject.Properties['Notes'] -and $tmplObj.Notes)         { "" + $tmplObj.Notes } else { $txtNotes.Text }
                $priority  = if ($tmplObj.PSObject.Properties['Priority'] -and $tmplObj.Priority)   { "" + $tmplObj.Priority } else { (if ($cmbPriority -and $cmbPriority.SelectedItem) { "" + $cmbPriority.SelectedItem } else { 'Normal' }) }

                $scheduledUtc = $null
                if     ($tmplObj.PSObject.Properties['ScheduledUtc'] -and $tmplObj.ScheduledUtc) { $scheduledUtc = ([DateTime]$tmplObj.ScheduledUtc).ToString("o") }
                elseif ($tmplObj.PSObject.Properties['Scheduled']    -and $tmplObj.Scheduled)    { $scheduledUtc = ([DateTime]$tmplObj.Scheduled).ToUniversalTime().ToString("o") }
                elseif ($dtp -and $dtp.Checked) { $scheduledUtc = $dtp.Value.ToUniversalTime().ToString("o") }

                $paramsObj = $null
                if ($script:paramControls -and $script:paramControls.Count -gt 0 -and $tmplObj.PSObject.Properties['ParameterDefs']) {
                    $tmp = @{}
                    foreach ($def in $tmplObj.ParameterDefs) {
                        $name = "" + $def.Name
                        if ([string]::IsNullOrWhiteSpace($name)) { continue }
                        $meta = $script:paramControls[$name]
                        if (-not $meta) { continue }
                        $type = "" + $meta.Type
                        $ctl  = $meta.Control
                        $val  = $null
                        switch ($type) {
                            'bool'        { $val = [bool]$ctl.Checked }
                            'int'         { $val = [int][decimal]$ctl.Value }
                            'number'      { $val = [double][decimal]$ctl.Value }
                            'select'      { $val = "" + $ctl.SelectedItem }
                            'multiselect' { $vals = @(); foreach ($ci in $ctl.CheckedItems) { $vals += "" + $ci }; $val = $vals }
                            'datetime'    { $val = $ctl.Value.ToString("o") }
                            default       { $val = "" + $ctl.Text }
                        }
                        $tmp[$name] = $val
                    }
                    $paramsObj = [pscustomobject]$tmp
                } elseif ($tmplObj.PSObject.Properties['Parameters'] -and $tmplObj.Parameters) {
                    $paramsObj = $tmplObj.Parameters
                } else {
                    $paramsObj = @{}
                }

                $targets = @()
                if ($chkAll.Checked) {
                    foreach ($item in $clbWorkers.Items) { $targets += "" + $item }
                } else {
                    foreach ($item in $clbWorkers.CheckedItems) { $targets += "" + $item }
                }
                if ($targets.Count -eq 0) { throw "Select at least one worker (or All)." }

                $reqDir = $cfg.Paths.Requests
                New-FTDirectory -Path $reqDir

                foreach ($w in $targets) {
                    $req = [pscustomobject]@{
                        Id             = [guid]::NewGuid().Guid
                        GroupId        = $groupId
                        Operation      = $operation
                        Template       = $template
                        Parameters     = $paramsObj
                        Notes          = $notes
                        RequestedBy    = $env:USERNAME
                        Priority       = $priority
                        SourceComputer = $env:COMPUTERNAME
                        Worker         = $w
                        CreatedUtc     = [DateTime]::UtcNow.ToString("o")
                        ScheduledUtc   = $scheduledUtc
                        Type           = "Request"
                    }
                    $fileName = "{0}_{1:yyyyMMddHHmmssfff}.json" -f $req.Id, (Get-Date)
                    $path = Join-Path $reqDir $fileName
                    $jsonOut = $req | ConvertTo-Json -Depth 10
                    $enc = New-Object System.Text.UTF8Encoding($true)
                    [System.IO.File]::WriteAllText($path, $jsonOut, $enc)
                    $statePath = [System.IO.Path]::ChangeExtension($path, '.state')
                    Write-FTState -Path $statePath -State New -Type "Request" -Percent 0 -Message "Created" -Worker $w -RequestId $req.Id
                }

                [System.Windows.Forms.MessageBox]::Show("Request(s) created.", "FSLogixTools", [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Information) | Out-Null
                $dlg.DialogResult = [System.Windows.Forms.DialogResult]::OK; $dlg.Close()
            } catch {
                [System.Windows.Forms.MessageBox]::Show("Failed to create request: $($_.Exception.Message)", "FSLogixTools - Error", [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Error) | Out-Null
            }
        })

        [void]$dlg.ShowDialog($Parent)
    
}
$btnNew.Add_Click({
    try { Show-NewRequestDialog -Parent $form -cfg $cfg } catch { [System.Windows.Forms.MessageBox]::Show("New Request failed: $($_.Exception.Message)") | Out-Null }
}# close Add_Click

    # perform an initial queue load before displaying the form
    & $loadQueue
    [void]$form.ShowDialog()
}

#endregion GUI

Export-ModuleMember -Function Start-FTAgent, Start-FTGui
