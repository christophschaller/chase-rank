$DataPath = "./data"
$DataPath = Resolve-Path $DataPath
$OsrmImage = "ghcr.io/project-osrm/osrm-backend"
$OsrmRegion = "baden-wuerttemberg-latest"
$OsrmMaxMatchingSize = 10000

docker run -t -i -p 5000:5000 -v "${DataPath}:/data" $OsrmImage osrm-routed --algorithm mld --max-matching-size $OsrmMaxMatchingSize /data/$OsrmRegion
