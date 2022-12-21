$DataPath = "/Users/david/dev_projects/chase-rank/data/osm/valhalla" #"./data/osm/valhalla"
$DataPath = Resolve-Path $DataPath
$ValhallaImage = "docker.pkg.github.com/gis-ops/docker-valhalla/valhalla:latest"


docker run -p 8002:8002 -v "${DataPath}:/custom_files" -e serve_tiles=True  -e build_admins=True  docker.pkg.github.com/gis-ops/docker-valhalla/valhalla:latest

#/Users/david/dev_projects/chase-rank/data/osm/valhalla