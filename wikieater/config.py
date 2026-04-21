from dataclasses import dataclass


@dataclass(frozen=True)
class GameSeed:
    name: str
    genre: str
    wiki_url: str
    perspective: str


# Filtered list: keeps games with inventory and first/third-person or group perspective.
FILTERED_GAMES: tuple[GameSeed, ...] = (
    GameSeed("Minecraft", "Sandbox & Survival", "https://minecraft.fandom.com/wiki/Minecraft_Wiki", "first/third"),
    GameSeed("Subnautica", "Sandbox & Survival", "https://subnautica.fandom.com/wiki/Subnautica_Wiki", "first"),
    GameSeed("Valheim", "Sandbox & Survival", "https://valheim.fandom.com/wiki/Valheim_Wiki", "third"),
    GameSeed("Baldur's Gate 3", "RPG", "https://bg3.wiki/wiki/Main_Page", "group"),
    GameSeed("The Witcher 3", "RPG", "https://witcher.fandom.com/wiki/The_Witcher_3:_Wild_Hunt", "third"),
    GameSeed("Skyrim", "RPG", "https://elderscrolls.fandom.com/wiki/The_Elder_Scrolls_V:_Skyrim", "first/third"),
    GameSeed("Elden Ring", "RPG", "https://eldenring.wiki.fextralife.com/Elden+Ring+Wiki", "third"),
    GameSeed("Fallout 4", "RPG", "https://fallout.fandom.com/wiki/Fallout_4", "first/third"),
    GameSeed("The Legend of Zelda: Breath of the Wild", "Action & Adventure", "https://zelda.fandom.com/wiki/The_Legend_of_Zelda:_Breath_of_the_Wild", "third"),
    GameSeed("Assassin's Creed Valhalla", "Action & Adventure", "https://assassinscreed.fandom.com/wiki/Assassin%27s_Creed_Valhalla", "third"),
    GameSeed("God of War (2018)", "Action & Adventure", "https://godofwar.fandom.com/wiki/God_of_War_(2018)", "third"),
    GameSeed("Borderlands 3", "Shooters", "https://borderlands.fandom.com/wiki/Borderlands_3", "first"),
    GameSeed("Call of Duty: Warzone", "Shooters", "https://callofduty.fandom.com/wiki/Call_of_Duty:_Warzone", "first/third"),
    GameSeed("Resident Evil Village", "Horror", "https://residentevil.fandom.com/wiki/Resident_Evil_Village", "first"),
    GameSeed("Silent Hill 2", "Horror", "https://silenthill.fandom.com/wiki/Silent_Hill_2", "third"),
    GameSeed("Amnesia: The Bunker", "Horror", "https://amnesia.fandom.com/wiki/Amnesia:_The_Bunker", "first"),
    GameSeed("Deus Ex: Mankind Divided", "Immersive Sims", "https://deusex.fandom.com/wiki/Deus_Ex:_Mankind_Divided", "first"),
    GameSeed("Prey (2017)", "Immersive Sims", "https://prey.fandom.com/wiki/Prey_(2017)", "first"),
    GameSeed("Dishonored 2", "Immersive Sims", "https://dishonored.fandom.com/wiki/Dishonored_2", "first"),
    GameSeed("Satisfactory", "Management/Automation", "https://satisfactory.wiki.gg/wiki/Satisfactory_Wiki", "first"),
    GameSeed("Metal Gear Solid V", "Stealth/Tactical", "https://metalgear.fandom.com/wiki/Metal_Gear_Solid_V:_The_Phantom_Pain", "third"),
    GameSeed("Hitman 3", "Stealth/Tactical", "https://hitman.fandom.com/wiki/HITMAN_3", "third"),
)

DEFAULT_THREADS = 2
DEFAULT_DB_PATH = "wikieater.sqlite3"
DEFAULT_WIKI_DIR = "wikis"
DEFAULT_REQUEST_TIMEOUT = 20
DEFAULT_SLEEP_SECONDS = 1.0
USER_AGENT = "WikiEaterBot/1.0 (+backup research; polite crawler)"
