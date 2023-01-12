def get_royalty_discount(game: str) -> float:
    GAME_ROYALTY_DICT = {
        'Harry Potter': 0.12,
        'Jurassic World Alive': 0.25,
        'Jurassic World the Game': 0.25
    }

    if game in GAME_ROYALTY_DICT:
        return 1 - GAME_ROYALTY_DICT[game]
    else:
        return 1.0