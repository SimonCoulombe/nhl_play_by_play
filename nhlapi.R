library(tidyverse)
library(jsonlite)
library(lubridate)
#https://github.com/sfrechette/nhlplaybyplay2-node/
getschedule <- function(season="20172018"){
  fromJSON(paste0("http://live.nhl.com/GameData/SeasonSchedule-", season ,".json")) 
}

schedule <- getschedule()  %>% mutate(datetime = as_datetime(est),
                          date = as.Date(datetime)) 
# for each game ID in the schedule, we will download the play by play data ,
# in list format (pbp) then wrangle the data to create two tables:
# one containing the events (one row per game_id + eventId)and one containing the events_players (one row per game_id + eventId + player_id)


myprocess  <- function(gameid){
  pbp <-  fromJSON(paste0("http://statsapi.web.nhl.com/api/v1/game/", gameid, "/feed/live"))

  # Some games such as "2017020018" are broken and have no play by play data.
  # skip the tibble creating part as required.
  #https://www.nhl.com/gamecenter/tbl-vs-fla/2017/10/07/2017020018#game=2017020018,game_state=final,game_tab=plays
  
  if (length(pbp$liveData$plays$allPlays) >0){
  
  allPlays <- pbp$liveData$plays$allPlays
  results <- pbp$liveData$plays$allPlays$result %>% select(-strength)
  strength <- pbp$liveData$plays$allPlays$result$strength  # strength est un data frame dans result
  about <- pbp$liveData$plays$allPlays$about %>% select(-goals)
  goals <- pbp$liveData$plays$allPlays$about$goals # goals est un data frame dans about
  coordinates <- pbp$liveData$plays$allPlays$coordinates
  players <- pbp$liveData$plays$allPlays$players
  team <- pbp$liveData$plays$allPlays$team
  colnames(strength) <- paste0("strength.", colnames(strength))
  colnames(goals) <- paste0("goals", colnames(goals))

  # base de données events
  mydf_events <- bind_cols(about , goals,
                           results , strength, 
                           coordinates, 
                           team) %>% 
    mutate(gamePk = pbp$gamePk) %>%
    select(gamePk, eventId, eventIdx, everything()) %>% as_tibble()
  
  #base de données player-events
  # il manque le event_id  (que je pourrais aller chercher dans about$eventId)
  # et game_id
  l <- list(players = players %>% map(function(x)bind_cols(as_tibble(x$player),as_tibble(x$playerType))), eventId = about$eventId, eventIdx = about$eventIdx, gamePk= pbp$gamePk)
  mydf_events_players <- pmap_df(l, function(players, eventId, eventIdx,gamePk) players  %>% 
                                   mutate(eventId =eventId, 
                                          eventIdx = eventIdx, 
                                          gamePk = gamePk )) %>%
    select(gamePk, eventId, eventIdx, everything())
  list(events = mydf_events, events_players = mydf_events_players)
} else {
  list(events = tibble(), events_players = tibble())
}
 
}

mydata100 <- schedule %>%  slice(1:100) %>% mutate(pbp=map(schedule$id, myprocess))
mydata200 <- schedule %>%  slice(101:200) %>% mutate(pbp=map(schedule$id, myprocess))
mydata300 <- schedule %>%  slice(201:300) %>% mutate(pbp=map(schedule$id, myprocess))
mydata400 <- schedule %>%  slice(301:400) %>% mutate(pbp=map(schedule$id, myprocess))
mydata500 <- schedule %>%  slice(401:500) %>% mutate(pbp=map(schedule$id, myprocess))
mydata600 <- schedule %>%  slice(501:600) %>% mutate(pbp=map(schedule$id, myprocess))
mydata700 <- schedule %>%  slice(601:700) %>% mutate(pbp=map(schedule$id, myprocess))
mydata800 <- schedule %>%  slice(701:800) %>% mutate(pbp=map(schedule$id, myprocess))
mydata900 <- schedule %>%  slice(801:900) %>% mutate(pbp=map(schedule$id, myprocess))
mydata1000 <- schedule %>%  slice(901:1000) %>% mutate(pbp=map(schedule$id, myprocess))
mydata1100 <- schedule %>%  slice(1001:1100) %>% mutate(pbp=map(schedule$id, myprocess))
mydata1200 <- schedule %>%  slice(1101:1200) %>% mutate(pbp=map(schedule$id, myprocess))
mydata1300 <- schedule %>%  slice(1201:1300) %>% mutate(pbp=map(schedule$id, myprocess))

mydata2 <- mydata %>% mutate(events = map(pbp, ~.x[["events"]]),
                             events_players = map(pbp, ~.x[["events_players"]])) 

mydf_events <-  mydata2$events %>% bind_rows()
mydf_events_players <- mydata2$events_players %>% bind_rows()


#exemple avec 1 match
game2015030411 <-  fromJSON("http://statsapi.web.nhl.com/api/v1/game/2015030411/feed/live")
#le JSON contient 6 listes : copyright, gamePk, link,  metaData, gameData, liveData
# je m'intéresse aux données play-by-play dans liveData$plays$allPlays
# allPlays est un data.frame qui contient 5 éléments
# result  (un data.frame, qui contient 10 variables, dont le data.frame strength)
# about (un data.frame)
# coordinates (un data.frame)
# players (une liste, qui peut etre NULL ou avoir deux data frames: player et playerType)
# team (un data.frame)

#J'aimerais avoir 3 bases: 
# 1)  une base "events" avec une ligne pour chaque événement (game_id, event_id, etc..)
# 2) une base "player-events", avec une ligne pour chaque joueur-événement (game_id, event_id, player_id, etc..) .  Par exemple, pour un but on a une ligne pour le buteur, une deuxième ligne pour le gardien, peut-être troisième ligne pour l'assist #1, peut-être une quatrième  ligne pour l'assist #2  
# 3) une base "player" avec les informations de chaque joueur (player_id, nom ,numéro, date de naissance, etc.. )

allPlays <- game2015030411$liveData$plays$allPlays
results <- game2015030411$liveData$plays$allPlays$result %>% select(-strength)
strength <- game2015030411$liveData$plays$allPlays$result$strength  # strength est un data frame dans result
about <- game2015030411$liveData$plays$allPlays$about %>% select(-goals)
goals <- game2015030411$liveData$plays$allPlays$about$goals # goals est un data frame dans about
coordinates <- game2015030411$liveData$plays$allPlays$coordinates
players <- game2015030411$liveData$plays$allPlays$players
team <- game2015030411$liveData$plays$allPlays$team
colnames(strength) <- paste0("strength.", colnames(strength))
colnames(goals) <- paste0("goals", colnames(goals))

# base de données events
mydf_events <- bind_cols(about , goals,
                  results , strength, 
                  coordinates, 
                  team) %>% 
  mutate(gamePk = game2015030411$gamePk) %>%
  select(gamePk, eventId, eventIdx, everything())
  
#base de données player-events
# il manque le event_id  (que je pourrais aller chercher dans about$eventId)
# et game_id
l <- list(players = players %>% map(function(x)bind_cols(as_tibble(x$player),as_tibble(x$playerType))), eventId = about$eventId, eventIdx = about$eventIdx, gamePk= game2015030411$gamePk)
mydf_events_players <- pmap_df(l, function(players, eventId, eventIdx, gamePk) players  %>% mutate(eventId =eventId, eventIdx = eventIdx, gamePk = gamePk )) %>%
  select(gamePk, eventId, eventIdx, everything())

#base de données players
## test test
mydf_players <-  map_df(game2015030411$gameData$players, function(x) as_tibble(x) %>% slice(4:4))  #creates 4 rows because of $current_team and $primaryposition

# approche #2
#  il existe des variable que je voudrais aller chercher, notamment
# currentTeamname =  x[["currentTeam$name"]],
# primaryPositioncode = x[["primaryPosition$code"]])
# mais je ne sais pas comment faire
# mydf_players <- game2015030411$gameData$players %>% map_df(function(x) tibble(
#   id = x[["id"]],
#   fullName = x[["fullName"]],
#   birthDate = x[["birthDate"]]))


# approche #3 cette ligne crée 240 observations à cause des 4 observations dans currentTeam
# mydf_players <- game2015030411$gameData$players %>%  map_df(function(x) as_tibble(x))

## plot shots

#point des shots + goals
ggplot(data = mydf_events %>% filter(event %in% c("Shot", "Goal")) %>%
         mutate(y = case_when(x<0 ~ -y,
                              TRUE ~ y),
                x = abs(x)),
       aes(x= abs(x), y=y, color = event)) +
  geom_point(alpha = 1/5)+
  geom_line(aes(x=89, y=y), color ="red")+
  geom_line(aes(x=25, y=y), color ="blue")+ 
  coord_cartesian(xlim=c(0,100)) 
mydf_events %>% summary

#hexbins des shots
ggplot(data = mydf_events %>% filter(event %in% c("Shot")) %>%
         mutate(y = case_when(x<0 ~ -y,
                              TRUE ~ y),
                x = abs(x)),
       aes(x= abs(x), y=y)) +
  geom_hex()+
  geom_line(aes(x=89, y=y), color ="red")+
  geom_line(aes(x=25, y=y), color ="blue")+ 
  coord_cartesian(xlim=c(0,100)) 



