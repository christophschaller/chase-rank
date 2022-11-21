import overpy

CATEGORIES = [
    "street",
    "gravel",
    "trail",
    "stairs"
]

WAY_SURFACE_MAP = {
    "street": [
        "asphalt",
        "concrete",
        "grass_paver",
        "paved",
        "paving_stones",
        "sett"
    ],
    "gravel": [
        "compacted",
        "fine_gravel",
        "gravel",
        "pebblestone",
        "unpaved",
        "rock",

    ],
    "dirt": [
        "dirt",
        "grass",
        "ground",
        "rubble"
    ]
}
SURFACE_WAY_MAP = {
    surface: way
    for way, surface_list in WAY_SURFACE_MAP.items()
    for surface in surface_list
}

WAY_HIGHWAY_MAP = {
    "dirt": [
        "path",
        "track"
    ],
    "street": [
        "service"
        "residential"
    ],
    "stairs": [
        "steps",
        "footway"
    ]
}
HIGHWAY_WAY_MAP = {
    surface: way
    for way, highway_list in WAY_HIGHWAY_MAP.items()
    for surface in highway_list
}

def categorize_way(way: overpy.Way) -> str:
    tags = way.tags
    if "surface" in tags:
        return WAY_SURFACE_MAP[tags["surface"]]

