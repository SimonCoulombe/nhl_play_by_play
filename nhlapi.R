
# load libraries ----
library(tidyverse)
library(jsonlite)
library(lubridate)


# define functions ----
get_schedule <- function(season="20172018"){
  fromJSON(paste0("http://live.nhl.com/GameData/SeasonSchedule-", season ,".json")) 
}
# tester 8468309
get_player_data <- function(id = "8475172"){ #Nazem Kadri  
  print(id)
  tmp <- fromJSON(paste0("https://statsapi.web.nhl.com/api/v1/people/", id ,".json")) %>% .[["people"]] 
  cn <- colnames(tmp)
  if ("currentTeam"  %in% cn ) {
    cT <- unnest(tmp$currentTeam)
    colnames(cT) <- paste0("currentTeam.", colnames(cT))
    tmp2 <- bind_cols(tmp %>%  select(-currentTeam, -primaryPosition), cT)
  } else{ tmp2 <- tmp %>%  select( -primaryPosition)}
  
  pP <- unnest(tmp$primaryPosition)
  colnames(pP) <- paste0("primaryPosition.", colnames(pP))
  tmp3 <- bind_cols(tmp2, pP)
  colnames(tmp3) <- paste0("player.", colnames(tmp3))
  tmp3
  }

get_data  <- function(gameid){
    print(gameid)
  fromJSON(paste0("http://statsapi.web.nhl.com/api/v1/game/", gameid, "/feed/live"))}
# pbp <- mydata[["pbp"]][[1]]
myprocess  <- function(pbp){
  print( pbp$gamePk)
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
  
  team <- pbp$liveData$plays$allPlays$team
  colnames(strength) <- paste0("strength.", colnames(strength))
  colnames(goals) <- paste0("goals.", colnames(goals))
  colnames(team) <- paste0("team.", colnames(team))

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
  players <- pbp$liveData$plays$allPlays$players
  players <- players %>% map(function(x) bind_cols(as_tibble(x$player),as_tibble(x$playerType))) 

  z <- map(players, function(x) {
    if(length(colnames(x))>0){
      colnames(x)<- paste0("player.", colnames(x))}
      x})
  
  l <- list(players = z,
            eventId = about$eventId, 
            eventIdx = about$eventIdx, 
            gamePk= pbp$gamePk)
  mydf_events_players <- pmap_df(l, function(players, eventId, eventIdx,gamePk) players  %>% 
                                   mutate(eventId =eventId, 
                                          eventIdx = eventIdx, 
                                          gamePk = gamePk )) %>%
    select(gamePk, eventId, eventIdx, everything())
  list(events = mydf_events, events_players = mydf_events_players)
} else {
  list(events = tibble(), events_players = tibble())
}}

# 1 - get game schedule ----

schedule <- get_schedule()  %>% mutate(datetime = as_datetime(est),
                                       date = as.Date(datetime))  %>% 
  filter( datetime< Sys.time() - 12*60*60)
#games that started at least 12 hours ago
# for each game ID in the schedule, we will download the play by play data ,
# in list format (pbp) then wrangle the data to create two tables:
# one containing the events (one row per game_id + eventId)and one containing the events_players (one row per game_id + eventId + player_id)

# 2 - download data from past games ----
mydata <- schedule %>%  mutate(pbp=map(id, get_data ))
saveRDS(mydata, "mydata.rds")
mydata <- readRDS("mydata.rds")
# 3 - process data ----
# original processing of data using purrr:map ----
 mydata2 <- mydata %>% mutate(processed = map(pbp , myprocess))
 mydata3 <- mydata2 %>% mutate(events = map(processed, ~.x[["events"]]),
                              events_players = map(processed, ~.x[["events_players"]])) 
 
 mydf_events <-  mydata3$events %>% bind_rows()
 mydf_events_players <- mydata3$events_players %>% bind_rows()

# current processing of data using parallel::parLapply ----
library(parallel)
  cl <- makeCluster(parallel:::detectCores() -1  )
  clusterEvalQ(cl, library("tidyverse"))
  clusterExport(cl, "mydata")
  mydata2 <- parLapply(cl = cl, X = mydata$pbp, fun = myprocess)
stopCluster(cl)
saveRDS(mydata2, "mydata2.rds")
mydata2 <- readRDS("mydata2.rds")
mydata3 <- mydata2 %>% transpose %>% as_tibble()
mydf_events <-  mydata3$events %>% bind_rows()
mydf_events_players <- mydata3$events_players %>% bind_rows()
# 4 get players data ----
#https://statsapi.web.nhl.com/api/v1/people/ID
players_data <- mydf_events_players %>% distinct(player.id) %>%     pull(player.id) %>% sort() %>%  map_df(get_player_data) 


saveRDS(players_data, "players_data.rds")

# 5 plot data ----
shots <- mydf_events %>% filter(event %in% c("Goal", "Shot", "Blocked Shot", "Missed Shot")) %>%
  filter(!is.na(x)) %>%
  mutate(
    y = case_when(x<0 ~ -y,TRUE ~ y),
    x = abs(x),
    success = ifelse(event == "Goal",1,0),
    angle = atan(y /(89-x)) * 180 / pi,
    distance = ((89-x)^2+ (y^2))^0.5) %>% 
  left_join(mydf_events_players %>% select(gamePk, eventId, eventIdx, player.id, player.value)) %>% 
  left_join(players_data) %>% filter(player.value %in% c("Shooter", "Scorer"))

hits <- mydf_events %>% filter(event %in% c("Hit")) %>%
  filter(!is.na(x)) %>%
  mutate(
    y = case_when(x<0 ~ -y,TRUE ~ y),
    x = abs(x),
    success = ifelse(event == "Goal",1,0),
    angle = atan(y /(89-x)) * 180 / pi,
    distance = ((89-x)^2+ (y^2))^0.5) %>% 
  left_join(mydf_events_players %>% select(gamePk, eventId, eventIdx, player.id, player.value)) %>% 
  left_join(players_data) %>% filter(player.value %in% c("Hitter"))



#hexbins des shots
ggplot(shots,
       aes(x= abs(x), y=y)) +
  geom_hex()+
  geom_line(aes(x=89, y=y), color ="red")+
  geom_line(aes(x=25, y=y), color ="blue")+ 
  coord_cartesian(xlim=c(0,100)) +
  facet_wrap(~ player.primaryPosition.code)#+
  #coord_flip() + scale_x_reverse()
ggsave("shots.png")

ggplot(hits,
       aes(x= abs(x), y=y)) +
  geom_hex()+
  geom_line(aes(x=89, y=y), color ="red")+
  geom_line(aes(x=25, y=y), color ="blue")+ 
  coord_cartesian(xlim=c(0,100)) +
  facet_wrap(~ player.primaryPosition.code)
ggsave("hits.png")

#hexbins des goals
ggplot(data = shots %>% filter(event == "Goal"),
       aes(x= abs(x), y=y)) +
  geom_hex()+
  geom_line(aes(x=89, y=y), color ="red")+
  geom_line(aes(x=25, y=y), color ="blue")+ 
  coord_cartesian(xlim=c(0,100))  +
  facet_wrap(~ player.primaryPosition.code)
ggsave("goals.png")

# hexplot taux de succès
ggplot(data = shots,
       aes(x= abs(x), y=y, z = success)) +
  stat_summary_hex( )+
  geom_line(aes(x=89, y=y), color ="red")+
  geom_line(aes(x=25, y=y), color ="blue")+ 
  coord_cartesian(xlim=c(0,100)) 
ggsave("goal_pct.png")

# 6 - model data ----

