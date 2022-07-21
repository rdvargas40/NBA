# NBA Datapipeline

## Objectivos

### Objetivo Principal
Orquestrar un flujo de datos que permita recargar una tabla analitica para entender la influencia que tienen el desempeño de las estadísticas individuales de los jugadores y colectivas de los equipos, en el resultado de un partido de baloncesto en la NBA.

### Objetivos Especificos
La base de datos debe permitir a un cientifico de datos responderse el siguiente tipo de preguntas:
- Entender las interacciones entre las diferentes estadísticas individuales del juego cómo asistencias, robos y puntos, con estadísticas grupales, en relación con el resultado final (victoria o derrota).
- Entender de que forma variables con las que operan los equipos, como el dinero que gastan en salarios, afectan el resultado de lkos partidos a lo largo de una temporada.
- Entender de que forma variables exogenas a los equipos, como el tamaño de la población y el ingreso por habitante del estado en el que operan, puede ser un factor que influencie los resultados de un equipo a lo largo de los años.

## Fuentes de Información
Para alcanzar estós objetivos se orquestro una base de datos alrededor de las siguientes fuentes de información:
- Api de la NBA: permite acceder a información sobre los jugadores, equipos y resultados de los partidos. Entrega niveles de detalles tales como el número de puntos y asistencias que consiguió cada jugador en cada partido.
- Dinero gastado por los equipos de la NBA, temporada a temporada. Esta información se toma haciendo web scraping de la pagina https://hoopshype.com
- Tamaño de la población e ingreso por habitante de cada estado. Viene deun archivo csv descargado de la pagina https://apps.bea.gov/itable/iTable.cfm?ReqID=70&step=1s

## Base de Datos
![GitHub Logo](https://github.com/rdvargas40/NBA/blob/stagging/images/nba%20database%20diagram.png)

## Diccionario de Datos

### Salarios (salaries)
- team_id: identidificador único de los equipos en la base de datos
- year: año en el que arranca la temporada, es un entero que inicia desde 1990.
- salaries: suma de los salarios de la nomina del equipo durante una temporada.

### Estados (states)
- state: nombre del estado de la unión americana
- year: año calendario
- personal_income: ingresos de las personas del estado durante el año ajustados por estacionalidad
- population: población del estado en la mitad del periodo
- pipc: ingreso promedio per capita

### Equipos (teams)
- team_id: identidificador único de los equipos en la base de datos
- full_name: nombre completo del equipo (Ej. Los Angeles Lakers)
- nick_name: apodo del equipo (Ej. Lakers)
- abbreviation: sigla de tres letras con la que se muestra al equipo en los marcadores de los partidos (Ej. LAL). También es única por equipo.
- city: ciudad en la que se encuentra la sede del equipo
- state: estado en el que se encuentr la sede del equipo
- year_founded: año en el que fue fundado el equipo

### Jugadores (players)
- player_id: indentificador único del jugador en la base de datos
- first_name: primer nombre del jugador
- last_name: apellido del jugador
- is_active: booleano que permite identificar si el jugador se encuentra en activo o esta retirado

### Partidos de los Equipos (teams_matches)
Esta tabla contiene las estadísticas de los resultados de los partidos a nivel de equipo
- team_id: identidificador único de los equipos en la base de datos
- match_id: identificador único de los partidos en la base de datos
- season_id: identificador único de la temporada en la base de datos
- game_date: fecha del partido
- opponent: oponente al que se enfrento el equipo en el partido, coincide con el campo abbreviation de la tabla teams
- result: "W" si el equipo ganó, "L" si perdió, este sería el indicador principal objetivo de análisis para un cientifico de datos.
- duration: duración en minutos del partido. En el baloncesto es muy variable en función de las faltas y los extra tiempos.
- points: puntos anotados por el equipo
- rebounds: rebotes capturados
- assists: asistencias de anotación.
- steals: robos de balón.
- blocks: bloqueos de anotación.
- turnovers: perdidas de balón sin disparo al tablero.

### Partidos de los Equipos (players_matches)
Esta tabla contiene las estadísticas de los resultados de los partidos a nivel de jugador individual
- player_id: identidificador único de los equipos en la base de datos
- match_id: identificador único de los partidos en la base de datos
- season_id: identificador único de la temporada en la base de datos
- minutes: número de minutos que el jugo en el partido. Debido a las rotaciones en un partido de baloncesto, esta cifra también es muy variable.
- points: puntos anotados por el jugador.
- rebounds: rebotes capturados
- assists: asistencias de anotación.
- steals: robos de balón.
- blocks: bloqueos de anotación.
- turnovers: perdidas de balón sin disparo al tablero.

## Intrucciones
- dwh_template.cfg: Este archivo es un template para las credenciales de la base de datos en PostgreSQL. El archivo real se llama dwh.cfg, para correr los códigos, tiene que cargar este archivo en el paquete y en las carpetas de las lambdas de AWS (app.py)



## Reglas de Calidad de Datos
Se van a implementar filtros en las tablas para evitar que llegen datos atípicos.

### Salarios
- Los salarios deben ser mayores a 0 y menores a 300 millones de dolares, por ahora. Este limite superior se debe ir actualizando.

### Estados
- Las distribuciones de las variables númericas siguen una tendencia más o menos lognormal. Es pertinente poner un límite inferior en 0, pero díficil poner un límite superior. Sin embargo, se podría revisar el comportamiento de las series de tiempo para colocar un límite superior que se vaya actualizando con el tiempo.

### Equipos
Se detecto que del api de la NBA el equipo Atlanta Hawks fue asignado a Atalanta como estado, cuando Atlanta es una ciudad. Se requieren limpiar estos datos para cambiar Atlanta por Georgia en el campo de estado. Esto se requiere para que haga match con la tabla de Estados. El equipo Toronto Raptors al ser de Canada no va a encontrar información en la tabla de estados.

### Partidos de Equipos
- Duración entre 50 y 400 minutos. Es muy variable debido a las faltas y las paradas del partido. Pero es imposible que dure menos de 50 minutos
- Puntos entre 30 y 200
- Asistencias de 0 a 80
- Robos de 0 a 40 
- Bloqueos de 0 a 40
- Perdidas de 0 a 50

### Partidos de Jugadores
Se colocan rangos superiores muy amplios debido a que siempore existe la pisibilidad de un partido extraordinario de un jugador.
- Tiempo de juego debe estar entre 0 y 100 minutos
- Puntos entre 0 y 120
- Rebotes entre 0 y 70
- Asistencias entre 0 y 60
- Robos entre 0 y 25
- Bloqueos entre 0 y 40
- Perdidas entre 0 y 40

