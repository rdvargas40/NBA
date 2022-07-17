"""
Descarga de información de las estdísticas de los jugadosres de la NBA partido a partido
"""

# Librerias
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.library.parameters import SeasonAll
from tqdm import tqdm



# Recorrido sobre todos los jugadores disponibles en la base de datos
for player in tqdm(players.get_players()):
    player_id = player['id']
    for _ in range(10):
      try:
        gamelog_player_all = playergamelog.PlayerGameLog(player_id=player_id, season=SeasonAll.all, timeout=10)
        df_player_games_all = gamelog_player_all.get_data_frames()[0]
        df_player_games_all.to_csv(f"data/players_matchs{player_id}.csv") 
        break
      except:
        continue