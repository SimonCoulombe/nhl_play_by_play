library(tidyverse)
library(jsonlite)
library(lubridate)
#https://github.com/sfrechette/nhlplaybyplay2-node/
getschedule <- function(season="20172018"){
  fromJSON(paste0("http://live.nhl.com/GameData/SeasonSchedule-", season ,".json")) 
}

schedule <- getschedule()  %>% mutate(datetime = as_datetime(est),
                          date = as.Date(datetime))  %>% 
  filter( date< Sys.Date())
# for each game ID in the schedule, we will download the play by play data ,
# in list format (pbp) then wrangle the data to create two tables:
# one containing the events (one row per game_id + eventId)and one containing the events_players (one row per game_id + eventId + player_id)



getdata  <- function(gameid){
print(gameid)
  fromJSON(paste0("http://statsapi.web.nhl.com/api/v1/game/", gameid, "/feed/live"))}


#myprocess  <- function(gameid){
myprocess  <- function(pbp){
  print( pbp$gamePk)
  #pbp <-  fromJSON(paste0("http://statsapi.web.nhl.com/api/v1/game/", gameid, "/feed/live"))

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
}}



#download data from past game
mydata <- schedule %>%  mutate(pbp=map(id, getdata))

# version originale 
# mydata2 <- mydata %>% mutate(processed = map(pbp , myprocess))
# mydata3 <- mydata2 %>% mutate(events = map(processed, ~.x[["events"]]),
#                              events_players = map(processed, ~.x[["events_players"]])) 
# 
# mydf_events <-  mydata3$events %>% bind_rows()
# mydf_events_players <- mydata3$events_players %>% bind_rows()

## alternative parallele
library(parallel)
  cl <- makeCluster(parallel:::detectCores()  )
  clusterEvalQ(cl, library("tidyverse"))
  clusterExport(cl, "mydata")
  mydata2 <- parLapply(cl = cl, X = mydata$pbp, fun = myprocess)
stopCluster(cl)

mydata3 <- mydata2 %>% transpose %>% as_tibble()
mydf_events <-  mydata3$events %>% bind_rows()
mydf_events_players <- mydata3$events_players %>% bind_rows()

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

#hexbins des goals
ggplot(data = mydf_events %>% filter(event %in% c("Goal")) %>%
         mutate(y = case_when(x<0 ~ -y,
                              TRUE ~ y),
                x = abs(x)),
       aes(x= abs(x), y=y)) +
  geom_hex()+
  geom_line(aes(x=89, y=y), color ="red")+
  geom_line(aes(x=25, y=y), color ="blue")+ 
  coord_cartesian(xlim=c(0,100)) 
# hexplot taux de succès
ggplot(data = mydf_events %>% filter(event %in% c("Goal", "Shot")) %>% 
         mutate(case_when( y>40 ~ 40,
                           y< -40 ~ -40,
                           TRUE ~ y)) %>%
         mutate(
                y = case_when(x<0 ~ -y,
                              TRUE ~ y),
                x = abs(x),
                success = ifelse(event == "Goal",1,0)),
       aes(x= abs(x), y=y, z = success)) +
  stat_summary_hex( )+
  geom_line(aes(x=89, y=y), color ="red")+
  geom_line(aes(x=25, y=y), color ="blue")+ 
  coord_cartesian(xlim=c(0,100)) 


