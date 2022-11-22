import overpy

WAY_CATEGORIES = [
    "street",
    "gravel",
    "trail",
    "stairs",
    "unknown"
]

# https://taginfo.openstreetmap.org/keys/surface
# surface is the most common tag we can use to determine a ways type
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

# https://taginfo.openstreetmap.org/keys/highway
# if the surface key is not set the highway key captures most ways
# we can't know the surface for sure but can still sort most of the remaining ways
WAY_HIGHWAY_MAP = {
    "dirt": [
        "path",
        "track",
        "unclassified"
    ],
    "street": [
        "service",
        "residential",
        "tertiary",
        "secondary",
        "primary",
        "crossing",
        "cycleway"
    ],
    "stairs": [
        "steps",
        "footway"
    ]
}
HIGHWAY_WAY_MAP = {
    highway: way
    for way, highway_list in WAY_HIGHWAY_MAP.items()
    for highway in highway_list
}

# https://taginfo.openstreetmap.org/keys/landuse
# if no highway key is set the majority of remaining ways is captured by landuse
# we don't know what the surface is, but we know what it is used for
WAY_LANDUSE_MAP = {
    "dirt": [
        "farmland",
        "grass",
        "forest",
        "meadow",
        "orchard",
        "farmyard",
    ],
    "street": [
        "residential",
        "industrial",
    ]
}
LANDUSE_WAY_MAP = {
    landuse: way
    for way, landuse_list in WAY_LANDUSE_MAP.items()
    for landuse in landuse_list
}


def categorize_way(way: overpy.Way) -> str:
    """
    Determines the category of a way and returns it as a string.
    """
    if not way:
        return "unknown"

    tags = way.tags
    if "surface" in tags:
        return SURFACE_WAY_MAP[tags["surface"]]
    elif "highway" in tags:
        return HIGHWAY_WAY_MAP[tags["highway"]]
    elif "landuse" in tags:
        return LANDUSE_WAY_MAP[tags["landuse"]]

    return "unknown"
