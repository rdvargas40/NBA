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
Para organizar estas fuentes de datos se eligió un modelo relacional. El diagrama se muestra a continuación:
![GitHub Logo](https://github.com/rdvargas40/NBA/blob/stagging/images/nba%20database%20diagram.png)

La base de datos se encuentra alojada en AWS RDS empleando PostgreSQL. Las razones de la elección son las siguientes:
- Garantizar la trazabilidad de las conexiones entre los datos.
- El volúmen actual de datos no es muy grande.
- El número de consultas que se requieren hacer al tiempo tampoco lo es.
A pesar de que el propósito del proyecto es la análitica, no se eligio un modelo tipo estrella con una tabla central, debido a que las preguntas que se buscan resolver requieren unionen entres tablas directamente relacionadas. Ahora, en el escenario alternativo de que esta tabla empiece a crecer de forma importante, se podría reevaluar esta desición en favor del modelo tipo estrella, e incluso considerar tecnologías como Redshift, sin embargo, esto se puede construir como una capa encima del modelo actual.


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

## Pipeline de Datos
El pipeline principal se organizó alrededor de la extracción de datos de la api de la NBA. Esto debido a que estas son las tablas más grandes. La tabla de partidos de jugadores tiene más de 1'200.000 registros. 

Las tabla de información de estados se alimenta manual a partir de un archivo .csv. Mientras que la tabla de salarios se alimenta por we scraping de una pagina web en un proceso que puede ser automatizado empleando metodologías similares a las empleadas en el pipeline principal (una Lambda de AWS puede hacer el trabajo). Esto último no se hizó en este proyecto por limitantes de tiempo.

Ahora, sobre el pipeline principal, el más complejo, consta de dos procesos paralelos: el de los jugadores y el de los equipos. A continuación se explica la lógica de actualización para los jugadores, el proceso más pesado. El de los equipos funciona de forma analoga.

![GitHub Logo](https://github.com/rdvargas40/NBA/blob/stagging/images/nba%20pipeline%20diagram.png)

El proceso consta de tres etapas, las dos primeras coordinadas por una Step Function de AWS.
1. Ejecución programada de una Lambda de AWS que actualiza la tabla de jugadores (players) y devuelve el listado de jugadores en activo para ser usados en el siguiente proceso.
2. Descarga de la información de los partidos recientes de todos los jugadores. El listado de jugadores en activo se usa como iterador por la AWS Step Function para ejecutar multiples la segunda AWS Lambda. Esto se hace de esta manera debido a que la descarga de la información de los partidos recientes de todos los jugadores es un proceso muy pesado y demorado para ser ejecutado en una sola ejecución de Lambda. En cambio lo que vamos a hacer es partir la ejecución para que se realice jugador a jugador. La AWS Step Function nos permite orquestrar este proceso de forma serverless en la nube. El resultado de la descarga de información de los partidos recientes de cada jugador se guarda en AWS S3.
3. Actualización de las Bases de Datos de Partidos. Ya por fuera de la AWS Step Function, existe una tercera Lambda que se activa cada vez que se deposita un archivo en la carpeta de partidos de jugadores del Bucket que tiene este proyecto en S3. Esta Lambda se activa, toma el archivo recien depositado y lo carga a la base de datos.

A continiación un diagrama de la Step Function:
![GitHub Logo](https://github.com/rdvargas40/NBA/blob/stagging/images/nba%20players%20step%20function%20diagram.png)

Este proceso se despliega en muy buena medida empleando el servicio AWS Serverless Application Model (SAM) (https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-resource-function.html) con instrucciones incluidas en este repositorio.

¿Por que se emplearon estas tecnologías?
- AWS Lambda: porque es serverless, no requiere servidores, solo ejecutar procesos cuando se necesitan. Muy practico para automatizar tareas.
- AWS Step Function: para coordinar ejecuciones en la nube.
- AWS SAM: permite desarrollar Lambdas y Step Functions dentro de un repositorio con control de cambios, y desplegarlas desde ese mismo ambiente.
- AWS S3: trazabilidad de información. Economico. Se integra facilmente con los demas servicios empleaaods en el pipeline.
- PostgreSQL: modelo realacional, con mucho soporte de desarrollo. Relativamente economico para el volumen de datos que esta manejando el pipeline.

## Intrucciones
### Inicialización de las Bases de Datos
Dentro del repositorio hay dos carpetas: initialization y pipelines. La primera contiene las funciones necesarias para crear y llenar las bases de datos, mientras que la segunda tiene las aplicaciones que se requieren desplegar en la nube para mantener las bases de datos actualizadas.

Dentro de initialization usualmente hay dos tipos de funciones por cada script (hay un script por tabla):
- Las de tipo create_table crea o inicializa la tabla.
- Las de tipo populate_table llenan la table con información histórica.

Ejemplos:
```python
teams.py
    create_teams_table # Crea la tabla de equipos
    populate_teams_table # Llena la tabla de equipos con información histórica
states.py
    create_states_table # Crea la tabla
    load_states_data # Llena la tabla de información histórica
salaries.py
    populate_database # Llena la talba de información histórica
players_matches.py
    populate_database # Llena la tabla de información histórica

```

### Confuguración del Pipeline
- dwh_template.cfg: Este archivo es un template para las credenciales de la base de datos en PostgreSQL. El archivo real se llama dwh.cfg, para correr los códigos, tiene que cargar este archivo en el paquete y en las carpetas de las lambdas de AWS (app.py)
- Asegurese de tener configuradas sus credenciales de AWS en el ambiente de desarrollo, en caso de que se encuentre trabajando en su computador local.
- Las Step Function se encuentran integradas dentro de los pipelines de cada proceso (players y teams). Los pipelines incluyen ademas de la step funcion (statemachine), la lambda que recoge los archivos de S3 y los actualiza  a la base de datos.
- Para desplegar un pipeline se requiere tener instalado el servicio SAM de AWS. Por favor siga las instrucciones de instalación en el enlace de la sección anterior.
- Por favor verifice que tiene AWS SAM instalado en su ambiente de desarrollo.
- Para desplegar un pipeline, ubique su directorio de trabajo en la carpeta del pipeline (Ej. pipelines/players/ o pipelines/teams/)
- Ejecute 
```bash
sam build
sam deploy --guided
```
- El pipeline (Step function y Lambdas) se encuentran desplegados en su cuenta de AWS.
- Hay dos lambdas cuyos triggers deben configurarse de forma manual en la consola de AWS:
    - update_players_matchs
    - update_teams_matchs
- Estas lambdas son las que recogen los archivos de S3. La razón es porque SAM solo puede configurar Triggers de aquellos servicios que se confuiguran en el repositorio. Estas dos Lambdas deben escuchar a eventos en S3 de tipo Put, que ocurran en el Bucket "nba.pipeline", dentro de las carpetas: "players/matchs" y "teams/matchs/" respectivamente.


## Reglas de Calidad de Datos
En la carpeta eda se encuentran notebooks por tipo de fuente de datos correspondientes al análisis exploratorio de la información histórica.

A partir de este análisis exploratorio se definen los siguientes filtros en las tablas para evitar que llegen datos de mala calidad.

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

## Escenarios Alternativos
1. Si los datos incrementaran en 100x.
    - El número de partidos por jugador no puede aumentar mucho más, asi que seria que aumenten el número de equipos y jugadores. Tal vez procesando la información de otras ligas o que la NBA admita equipos de otras ciudades. El punto que quiero hacer es que si el número de partidos por jugador se mantiene en ese orden de magnitud, y lo mismo pasa con los equipos, la solución del pipeline sería robusta frente a un aumentos de equipos (y por ende de jugadores).
    - Lo que si se puede empezar a quedar la base de datos. PostgreSQL no esta optimizado para manejar grandes volumnees de información. Esto se va a ver representado en rendimiento y en costos. Una alternativa frente a esto como ya se menciono previamente es Redshift. Entiendo que esta optimizado para hacer queries en tablas más grandes de formas más eficiente ahorrando costos y tiempo.

2. Si el pipeline se ejecutara diariamente en una ventana de tiempo especifica.
    - Desde el punto de vista de arquitectura del pipeline de la Step Function se podría orquestrar la ejecución de Lambda 2X en forma asincrona y en lugar de hacerlo en serie. Sin embargo, hay que probar si la api de la NBA soporta esa cantidad de solicitudes.

3. Si la base de datos necesitara ser atendida por más de 100 usuarios funcionales
    - Esta solución claramente no fue pensada para una necesidad de ese estilo. En un caso de eso con un objetivo distinto, que no necesariamente tiene que ser el de análitica, podría pensarse en una base de datos no relacional.
    - Preveer que tipo de solicitudes requieren hacer los usuarios y hacer vistas preprocesadas de la tabla principal.

4. Si se requiere hacer analítica en tiempo real
    - El proceso completo fue diseñado en batch, incluyendo el propósito, así que habría que cambiar muchos componentes, pero empezando con el propósito.
    - Está Kinesis, el servicio de recolección y procesamiento de streaming de datos de AWS: https://docs.aws.amazon.com/streams/latest/dev/introduction.html
    - Hay múltiples aplicaciones desde dashboards, hasta aplicaciones de analítica
    - Kinesis también puede coordinarse para activar funciones Lambda: https://docs.aws.amazon.com/es_es/lambda/latest/dg/with-kinesis.html