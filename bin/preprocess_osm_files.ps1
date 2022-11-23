$DataPath = "./data/osm"
$DataPath = Resolve-Path $DataPath
$OsrmProfile = "/opt/bicycle.lua"
$OsrmImage = "ghcr.io/project-osrm/osrm-backend"

Get-ChildItem $DataPath -Filter *.osm.pbf | ForEach-Object {
    $FileStem = (Split-Path -Path $_ -Leaf).Split(".")[0]
    Write-Output "extracting: $FileStem"
    docker run -t -v "${DataPath}:/data" $OsrmImage osrm-extract -p $OsrmProfile /data/${FileStem}.osm.pbf
    docker run -t -v "${DataPath}:/data" $OsrmImage osrm-partition /data/${FileStem}.osrm
    docker run -t -v "${DataPath}:/data" $OsrmImage osrm-customize /data/${FileStem}.osrm
}
