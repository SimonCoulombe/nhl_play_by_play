
Sys.setenv(LANGUAGE = "en")



# load libraries ----
library(speedglm)
library(jsonlite)
library(lubridate)
library(ggforce)
library(data.table)
library(tidyverse)
library(furrr)
library(tictoc)
library(lightgbm)
plan(multiprocess(workers=8))

# define functions ----
get_schedule <- function(season = "20172018") {
  fromJSON(paste0(
    "http://live.nhl.com/GameData/SeasonSchedule-",
    season ,
    ".json"
  )) %>%
    mutate(est = as_datetime(est),
           date = as.Date(est))
}
# tester 8468309
get_player_data <- function(id = "8475172") {
  #Nazem Kadri
  print(id)
  tmp <-
    fromJSON(paste0("https://statsapi.web.nhl.com/api/v1/people/", id , ".json")) %>% .[["people"]]
  cn <- colnames(tmp)
  if ("currentTeam"  %in% cn) {
    # currentTeam is a nested data.frame, unnest and bind
    # some players no longer have a team,
    # so they don't have the currentTeam nested data.frame
    cT <- tmp$currentTeam
    colnames(cT) <- paste0("currentTeam.", colnames(cT))
    tmp2 <-
      bind_cols(tmp %>%  select(-currentTeam, -primaryPosition), cT)
  } else{
    tmp2 <- tmp %>%  select(-primaryPosition)
  }
  # primaryPosition is a nested data.frame, unnest and bind
  pP <- tmp$primaryPosition
  colnames(pP) <- paste0("primaryPosition.", colnames(pP))
  tmp3 <- bind_cols(tmp2, pP)
  colnames(tmp3) <- paste0("player.", colnames(tmp3))
  tmp3
}

get_data  <- function(gameid) {
  print(gameid)
  fromJSON(paste0(
    "http://statsapi.web.nhl.com/api/v1/game/",
    gameid,
    "/feed/live"
  ))
}

get_shift_data <- function(gameid) {
  #print(gameid)
  fromJSON(
    paste0(
      "http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId=",
      gameid
    )
  )  %>% .[["data"]] %>% as_tibble() %>%
    mutate(gamePk = as.integer(gameid))
}


process_data  <- function(pbp) {
  print(pbp$gamePk)
  # lots of nested data frames in the play-by-play data, we'll need to unnest and bind.
  # Some games such as "2017020018" are broken and have no play by play data.
  # skip the tibble creating part as required.
  #https://www.nhl.com/gamecenter/tbl-vs-fla/2017/10/07/2017020018#game=2017020018,game_state=final,game_tab=plays
  if (length(pbp$liveData$plays$allPlays) > 0) {
    allPlays <- pbp$liveData$plays$allPlays
    results <-
      pbp$liveData$plays$allPlays$result %>% select(-strength)
    strength <-
      pbp$liveData$plays$allPlays$result$strength  # strength is a nested data.frame in result
    about <- pbp$liveData$plays$allPlays$about %>% select(-goals)
    goals <-
      pbp$liveData$plays$allPlays$about$goals # goals is a nested data.frame in about
    coordinates <- pbp$liveData$plays$allPlays$coordinates
    
    team <- pbp$liveData$plays$allPlays$team
    colnames(strength) <- paste0("strength.", colnames(strength))
    colnames(goals) <- paste0("goals.", colnames(goals))
    colnames(team) <- paste0("team.", colnames(team))
    
    # create events data frame
    mydf_events <- bind_cols(about , goals,
                             results , strength,
                             coordinates,
                             team) %>%
      mutate(gamePk = pbp$gamePk) %>%
      select(gamePk, eventId, eventIdx, everything()) %>% as_tibble()
    
    # create events_players data frame
    players <- pbp$liveData$plays$allPlays$players
    # players is a list  has two nested data frames for each player, $player and $playerType, bind them
    players <-
      players %>% map(function(x)
        bind_cols(as_tibble(x$player), as_tibble(x$playerType)))
    
    z <- map(players, function(x) {
      if (length(colnames(x)) > 0) {
        colnames(x) <- paste0("player.", colnames(x))
      }
      x
    })
    
    # add the gamePk (game id) and eventId to the player event data
    l <- list(
      players = z,
      eventId = about$eventId,
      eventIdx = about$eventIdx,
      gamePk = pbp$gamePk
    )
    mydf_events_players <-
      pmap_df(l, function(players, eventId, eventIdx, gamePk)
        players  %>%
          mutate(
            eventId = eventId,
            eventIdx = eventIdx,
            gamePk = gamePk
          )) %>%
      select(gamePk, eventId, eventIdx, everything())
    #return ta list of two tibbles, one for events and one for events_players
    list(events = mydf_events, events_players = mydf_events_players)
  } else {
    #if no play by play data, return a list of empty tibbles
    list(events = tibble(), events_players = tibble())
  }
}

# Example - single game use ----
schedule <- get_schedule(season = "20182019")
single_game_data <- get_data("2018020182")
single_game_processed <- process_data(single_game_data)
events <- single_game_processed$events
events_players <- single_game_processed$events
single_players_data <- get_player_data("8475172")

# Example - purrr the whole season----

# ** 1 - get game schedule ----

schedule <-
  get_schedule(season = "20182019")  %>% filter(est < Sys.time() - 12 * 60 * 60)
# keep games that started at least 12 hours ago, because games in progress have
# events, but don't have the "strength" nested data.frame

# For each game ID in the schedule, we will download the play by play data ,
# in list format (pbp) then wrangle the data to create two tables:
# one containing the events (one row per game_id + eventId)and one containing the events_players (one row per game_id + eventId + player_id)

# ** 2 - download data from past games ----

tic()
game_data <- schedule %>%   mutate(pbp = future_map(id, get_data, .progress = TRUE))
toc() # 39s sur8 coeurs
write_rds(game_data, "game_data.rds") # 360s sur 1 coeur 
# ** 3 - process data ----
#game_data <- read_rds("game_data.rds")
# **** 3A multicore furrr::future_map ----



processed_data <- game_data %>% 
  mutate(processed = future_map(pbp , process_data, .progress = TRUE)) %>%
  mutate(events = future_map(processed, ~ .x[["events"]], .progress = TRUE),
         events_players = future_map(processed, ~ .x[["events_players"]], .progress = TRUE)
  )

mydf_events <-  processed_data$events %>% bind_rows()
mydf_events_players <- processed_data$events_players %>% bind_rows()
write_rds(processed_data, "processed_data.rds")
write_rds(mydf_events, "mydf_events.rds")
write_rds(mydf_events_players, "mydf_events_players.rds")



# **** 3B parallel::parLapply ----
# 
# library(parallel)
# cl <- makeCluster(parallel:::detectCores() - 1)
# clusterEvalQ(cl, library("tidyverse"))
# clusterExport(cl, "game_data")
# processed_data_parallel_tmp <-
#   parLapply(cl = cl, X = game_data$pbp, fun = process_data)
# stopCluster(cl)
# processed_data_parallel <-
#   processed_data_parallel_tmp %>%  transpose %>% as_tibble()
# saveRDS(processed_data_parallel, "processed_data_parallel.rds")
# 
# mydf_events_parallel <-
#   processed_data_parallel$events %>% bind_rows()
# mydf_events_players_parallel <-
#   processed_data_parallel$events_players %>% bind_rows()

# ** 4 get data for players involved in any game ----
players_data <- mydf_events_players %>%
  distinct(player.id) %>%
  pull(player.id) %>%
  sort() %>%
  future_map(get_player_data) %>% bind_rows

write_rds(players_data, "players_data.rds")

# ** 5 plot data ----

processed_data <- readRDS("processed_data.rds")
mydf_events <- readRDS("mydf_events.rds")
mydf_events_players <- readRDS("mydf_events_players.rds")
players_data <- readRDS("players_data.rds")
#note :blocked shots are awarded to the blocking team, not the shooting team.
# coordinates of blocked shots appear to refer to the location of the block, not of the shot.
# i say this because the coordinates for defensemen shooter are right in front of the net.
shots <-
  mydf_events %>% filter(event %in% c("Goal", "Shot", "Missed Shot")) %>% #, "Blocked Shot"
  filter(!is.na(x)) %>%
  mutate(
    y = case_when(x < 0 ~ -y, TRUE ~ y),
    #rotate shots so that everything is in the same end of the rink.
    x = abs(x),
    success = ifelse(event == "Goal", 1, 0),
    angle = atan(y / (89 - x)) * 180 / pi,
    distance = ((89 - x) ^ 2 + (y ^ 2)) ^ 0.5
  ) %>%
  left_join(mydf_events_players %>% select(gamePk, eventId, eventIdx, player.id, player.value)) %>%
  left_join(players_data) %>% filter(player.value %in% c("Shooter", "Scorer"))

misses <- mydf_events %>% filter(event %in% c("Missed Shot")) %>%
  filter(!is.na(x)) %>%
  mutate(
    y = case_when(x < 0 ~ -y, TRUE ~ y),
    #rotate shots so that everything is in the same end of the rink.
    x = abs(x),
    success = ifelse(event == "Goal", 1, 0),
    angle = atan(y / (89 - x)) * 180 / pi,
    distance = ((89 - x) ^ 2 + (y ^ 2)) ^ 0.5
  ) %>%
  left_join(mydf_events_players %>% select(gamePk, eventId, eventIdx, player.id, player.value)) %>%
  left_join(players_data) %>% filter(player.value %in% c("Shooter", "Scorer"))


blocks <- mydf_events %>% filter(event %in% c("Blocked Shot")) %>%
  filter(!is.na(x)) %>%
  mutate(
    y = case_when(x < 0 ~ -y, TRUE ~ y),
    #rotate shots so that everything is in the same end of the rink.
    x = abs(x),
    success = ifelse(event == "Goal", 1, 0),
    angle = atan(y / (89 - x)) * 180 / pi,
    distance = ((89 - x) ^ 2 + (y ^ 2)) ^ 0.5
  ) %>%
  left_join(mydf_events_players %>% select(gamePk, eventId, eventIdx, player.id, player.value)) %>%
  left_join(players_data) %>% filter(player.value %in% c("Shooter", "Scorer"))

hits <- mydf_events %>% filter(event %in% c("Hit")) %>%
  filter(!is.na(x)) %>%
  mutate(
    y = case_when(x < 0 ~ -y, TRUE ~ y),
    x = abs(x),
    success = ifelse(event == "Goal", 1, 0),
    angle = atan(y / (89 - x)) * 180 / pi,
    distance = ((89 - x) ^ 2 + (y ^ 2)) ^ 0.5
  ) %>%
  left_join(mydf_events_players %>% select(gamePk, eventId, eventIdx, player.id, player.value)) %>%
  left_join(players_data) %>% filter(player.value %in% c("Hitter"))

# ice features
faceoff_circles <- data.frame(
  x0 = c(0, 69, 69, 20, 20),
  y0 = c(0, 22,-22, 22,-22),
  r = c(15, 15, 15, 2, 2)
) #neutral should be 1, but too small for my liking

goal_rectangle <- data.frame(
  xmin = 89,
  xmax = 91,
  ymin = -3,
  ymax = 3
)

blue_line <- data.frame(x = c(25, 25),
                        y = c(-40, 40))

goal_line <- data.frame(x = c(89, 89),
                        y = c(-40, 40))



#hexbins des shots
ggplot(shots  %>% filter(player.primaryPosition.code != "G")) +
  geom_hex(aes(x = x, y = y), bins = 25) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100)) +
  coord_fixed(ratio = 1) +
  facet_wrap(~ player.primaryPosition.code) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Shots origin by position, 2017-2018 season",
    caption = paste0("(based on data from NHL API as of ", Sys.Date(), ")")
  )
ggsave("shots.png")

#mcdavid shots
ggplot(shots  %>% filter(player.fullName == "Connor McDavid")) +
  geom_hex(aes(x = x, y = y), bins = 25) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100)) +
  coord_fixed(ratio = 1) +
  facet_wrap(~ player.primaryPosition.code) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Connor McDavid shots by location, 2017-2018 season",
    caption = paste0("(based on data from NHL API as of ", Sys.Date(), ")")
  )
ggsave("mcdavid_shots.png")

#hexbins des hits
ggplot(hits %>% filter(player.primaryPosition.code != "G" %>% head(1000))) +
  geom_hex(aes(x = x, y = y), bins = 25) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100)) +
  coord_fixed(ratio = 1) +
  facet_wrap(~ player.primaryPosition.code) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Hits location by position, 2017-2018 season",
    caption = paste0("(based on data from NHL API as of ", Sys.Date(), ")")
  )

ggsave("hits.png")

#hexbins des goals
ggplot(data = shots %>% filter(event == "Goal") %>% filter(player.primaryPosition.code != "G")) +
  geom_hex(aes(x = x, y = y), bins = 25) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100))  +
  coord_fixed(ratio = 1) +
  facet_wrap(~ player.primaryPosition.code) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Goals location by position, 2017-2018 season",
    caption = paste0("(based on data from NHL API as of ", Sys.Date(), ")")
  )
ggsave("goals.png")


ggplot(data = shots %>% filter(event == "Goal") %>% filter(player.fullName == "Connor McDavid")) +
  geom_hex(aes(x = x, y = y), bins = 25) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100))  +
  coord_fixed(ratio = 1) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Connor McDavid Goals by location, 2017-2018 season",
    caption = paste0("(based on data from NHL API as of ", Sys.Date(), ")")
  )
ggsave("mcdavid_goals.png")


# hexplot taux de succès
capped_mean <- function(x) {
  if (length(x) > 10) {
    mean_x = mean(x)
    mean_x #  min(mean_x, 0.25)
  } else{
    NA
  }
}

ggplot() +
  stat_summary_hex(
    data = shots %>% filter(is.na(emptyNet) | emptyNet == FALSE),
    bins = 25,
    aes(x = x, y = y, z = success),
    fun = capped_mean
  ) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100)) +
  coord_fixed(ratio = 1) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Shot efficiency by location, 2017-2018 season",
    caption = paste0(
      "(minimum of 10 shots per hex, based on data from NHL API as of ",
      Sys.Date(),
      ")"
    )
  )
ggsave("goal_pct.png")



# 6 wrangle shifts data  ----
# 
schedule <- get_schedule(season = "20182019")
players_data <- readRDS("players_data.rds")
game_data <- readRDS("game_data.rds")
events <- readRDS("mydf_events.rds")

shift_data <- schedule %>%
  mutate(shift = future_map(id, get_shift_data, .progress= TRUE)) # error vector too big?
  #mutate(shift = map(id, get_shift_data))

write_rds(shift_data, "shift_data20182019.rds")


shift_data <- read_rds("shift_data20182019.rds")  %>% as_tibble()


z <-
  map_df(shift_data$shift, function(X) {
    dim(X) %>%  t() %>% as_tibble
  })   # get shift_data dimension
z %>% filter(V1 == 0) # t games have no shift.. .the last 10 .. let's drop them when wrangling shift data
z <- z %>% mutate(keep = V1 > 0)
# the 3 all star games (atlantic, pacific metropolitamn 2017040631, 2017040632 , 2017040633) and playoff games from mai 12 onward dont have shifts
# we'll just ignore them going forward.


X <- shift_data$shift[[33]]

tdeb <- Sys.time()
#cl <- parallel::makeForkCluster(parallel::detectCores() - 1)
cl <- parallel::makeForkCluster(30)
model_data  <-
  parallel::parLapply(
    cl = cl,
    X = shift_data$shift[z$keep],
    #X = shift_data$shift[1:30],
    fun = function(X) {
      result <- list()
      
      thisgame <- X %>% distinct(gamePk)
      
      goals <-
        events %>% filter(gamePk %in% thisgame$gamePk, event == "Goal") %>%
        mutate(time = as.integer(str_sub(periodTime, 1, 2)) * 60 +  as.integer(str_sub(periodTime, 4, 5))) %>%
        select(gamePk, period, time, description, team.triCode)  %>%
        left_join(schedule %>% select(gamePk = id, a, h)) %>%
        mutate(home_away = ifelse(team.triCode == h, "HOME", "AWAY"))
      
      if (TRUE) {
        skaters_shifts <-
          X %>%   left_join(shift_data %>% select(gamePk = id, h, a)) %>%
          left_join(
            players_data %>% select(playerId = player.id, player.fullName, player.primaryPosition.code)
          ) %>%
          select(
            gamePk,
            teamName,
            period,
            startTime,
            endTime,
            shiftNumber,
            lastName,
            player.primaryPosition.code,
            everything()
          ) %>% arrange(teamName, period, startTime) %>%
          
          mutate(home_away = ifelse(teamAbbrev == h, "HOME", "AWAY")) %>%
          # enlève les gardiens
          #filter(player.primaryPosition.code != "G") %>%
          
          # crée int_start_time et int_end_time
          mutate(
            int_start_time = as.integer(str_sub(startTime, 1, 2)) * 60 +  as.integer(str_sub(startTime, 4, 5)) ,
            int_end_time = as.integer(str_sub(endTime, 1, 2)) * 60 +  as.integer(str_sub(endTime, 4, 5))
          ) %>%
          filter(int_start_time < int_end_time) %>% # sttrange shifts with no duration appear to be goals. TODO: understand this. %>%
          select(
            gamePk,
            home_away,
            period,
            int_start_time,
            int_end_time,
            lastName,
            playerId,
            player.primaryPosition.code
          )
        
        skaters_seconds_played <- skaters_shifts %>%
          mutate(data = map2(int_start_time, int_end_time, ~ {
            ## Génération des secondes du shift
            data_frame(time = seq(
              from = .x + 1,
              to = .y,
              by = 1
            ))
          })) %>%
          select(-int_end_time,-int_start_time)  %>%
          unnest(data) %>%
          select(gamePk, period, time, home_away, playerId) %>% 
          arrange(period, time, home_away, playerId) %>%  
          group_by(gamePk, period, time) %>%
          nest() %>%
          #nest(-gamePk,-period, -time)  %>%
          arrange(gamePk, period, time) %>%
          left_join(goals %>% select(gamePk, period, time, home_away))
        
        
        shifts <- skaters_seconds_played %>%
          mutate (
            team =  map(data, ~ .x[["home_away"]]),
            players = map(data, ~ .x[["playerId"]]),
            lagplayers = lag(players),
            lagperiod = lag(period),
            laggamePk = lag(gamePk),
            #same = map2(players,lagplayers, function(x,y){ identical(x,y)}),
            same = pmap(list(players, lagplayers, period, lagperiod, gamePk, laggamePk), function(players,
                                                                                                  lagplayers,
                                                                                                  period,
                                                                                                  lagperiod,
                                                                                                  gamePk,
                                                                                                  laggamePk) {
              identical(players, lagplayers) &
                identical(period, lagperiod) &
                identical(gamePk, laggamePk)
            }),
            shift =  cumsum(same == FALSE)
          ) %>%
          select(-lagplayers, -data, -lagperiod,-laggamePk) %>%
          group_by(shift) %>%
          mutate (
            int_start_time = min(time),
            ## i'd rather summarise than mutate+filter, but I cant group by players because it is a list
            int_end_time = max(time),
            home_away = case_when(
              any(home_away == "HOME") ~ "HOME",
              any(home_away == "AWAY") ~ "AWAY",
              TRUE ~ NA_character_
            )
          ) %>%
          ungroup()   %>%
          filter(same == FALSE) %>%
          mutate(duration = int_end_time - int_start_time + 1) %>%
          select(-same, -time)
        
        
        create2shifts <- shifts %>% mutate(
          homeplayers = map2(players, team, function(players, team) {
            players[team == "HOME"]
          }),
          awayplayers = map2(players, team, function(players, team) {
            players[team == "AWAY"]
          }),
          homegoal = case_when(home_away == "HOME" ~ 1, TRUE ~ 0),
          awaygoal = case_when(home_away == "AWAY" ~ 1, TRUE ~ 0)
        )  %>%
          select(
            gamePk,
            shift,
            period,
            int_start_time,
            int_end_time,
            homeplayers,
            awayplayers,
            homegoal,
            awaygoal,
            duration
          ) %>%
          mutate(strength_home = map(homeplayers, function(X) {
            length(X)
          })) %>% unnest(strength_home) %>%
          mutate(strength_away = map(awayplayers, function(X) {
            length(X)
          })) %>% unnest(strength_away)
        ## ok on appliquel e stack overflow
        
        
        model_data_home <- create2shifts %>%
          select(
            gamePk,
            shift,
            period,
            duration,
            strength_home,
            strength_away,
            for_goal = homegoal,
            for_players = homeplayers,
            against_players = awayplayers,
            for_strength = strength_home,
            against_strength = strength_away
          ) %>%
          mutate(for_players = map(for_players,unique), against_players = map(against_players, unique)) %>% # prevent error when spreading duplicated in 20182019
          unnest(for_players, .drop = F) %>%
          spread(for_players, for_players, sep = '_') %>%
          unnest(against_players, .drop = F) %>%
          spread(against_players, against_players, sep = '_')
        
        model_data_away <- create2shifts %>%
          select(
            gamePk,
            shift,
            period,
            duration,
            strength_home,
            strength_away,
            for_goal = awaygoal,
            for_players = awayplayers,
            against_players = homeplayers,
            for_strength = strength_away,
            against_strength = strength_home
          ) %>%
          mutate(for_players = map(for_players,unique), against_players = map(against_players, unique)) %>%
          unnest(for_players, .drop = F) %>%
          spread(for_players, for_players, sep = '_') %>%
          unnest(against_players, .drop = F) %>%
          spread(against_players, against_players, sep = '_')
        
        model_data <-
          bind_rows(model_data_home, model_data_away)
        result <- model_data
        
      }
      result
    }
  )

parallel::stopCluster(cl)
tfin <- Sys.time() # 2 minutes on 31 cores, up to 50 GB RAM use
as.numeric(tfin - tdeb, units = "mins")

saveRDS(model_data, file = "model_data20182019.rds") # 730 000 rows



tdeb <- Sys.time()
cl <- parallel::makeForkCluster(12)
total_time_played  <-
  parallel::parLapply(
    cl = cl,
    X = shift_data$shift[z$keep],
    fun = function(X) {
      result <- list()
      
      thisgame <- X %>% distinct(gamePk)
      
      if (TRUE) {
        skaters_shifts <-
          X %>%   left_join(shift_data %>% select(gamePk = id, h, a)) %>%
          left_join(
            players_data %>% select(playerId = player.id, player.fullName, player.primaryPosition.code)
          ) %>%
          select(
            gamePk,
            teamName,
            period,
            startTime,
            endTime,
            shiftNumber,
            lastName,
            player.primaryPosition.code,
            everything()
          ) %>% arrange(teamName, period, startTime) %>%
          
          mutate(home_away = ifelse(teamAbbrev == h, "HOME", "AWAY")) %>%
          # enlève les gardiens
          
          #filter(player.primaryPosition.code != "G") %>%
          # crée int_start_time et int_end_time
          mutate(
            int_start_time = as.integer(str_sub(startTime, 1, 2)) * 60 +  as.integer(str_sub(startTime, 4, 5)) ,
            int_end_time = as.integer(str_sub(endTime, 1, 2)) * 60 +  as.integer(str_sub(endTime, 4, 5)),
            duration =  int_end_time - int_start_time + 1
          ) %>%
          filter(int_start_time < int_end_time) %>% # sttrange shifts with no duration appear to be goals. TODO: understand this. %>%
          select(
            gamePk,
            home_away,
            period,
            int_start_time,
            int_end_time,
            duration,
            lastName,
            playerId,
            player.primaryPosition.code
          )
        
        result <-
          skaters_shifts %>% group_by(playerId) %>% summarise(duration = sum(duration)) %>% ungroup()
      }
      result
    }
  )

parallel::stopCluster(cl)

total_time_played <-
  total_time_played %>% bind_rows %>% group_by(playerId) %>% summarise(duration = sum(duration))
tfin <- Sys.time() # 2 minutes on 31 cores, up to 50 GB RAM use
as.numeric(tfin - tdeb, units = "mins")

saveRDS(total_time_played, file = "total_time_played20182019.rds") # 730 000 rows

#7 - model goals using shift data ----

#total_time_played <- readRDS( "total_time_played") # 730 000 rows
model_data <- readRDS( "model_data20182019.rds") # 730 000 rows

model_data_df <-
  model_data %>% bind_rows %>% 
  mutate_at(vars(starts_with("against_players") , starts_with("for_players")), 
            funs(as.integer(!is.na(.)))) %>%  # convert dummy to 01/ 
  filter(for_strength == 6, against_strength == 6)




dummyvars <-
  model_data_df %>% select(starts_with("against_players") , starts_with("for_players")) %>% 
  #select(-against_players_8474056, -for_players_8474056) %>% colnames # P.K. Subban is reference.
  colnames()

fla <-
  paste("for_goal ~  offset(log(duration)) +",
        #paste(dummyvars, collapse = "+")) %>% as.formula(.)
        paste(dummyvars, collapse = "+")) %>% as.formula(.)



## lightgbm

library(lightgbm)
library(Matrix)
param <- list(num_leaves = 2, # num_leaves = 2^(max_depth).
              learning_rate = 0.01,
              nthread = 29,
              objective = "poisson",
              feature_fraction = 0.8,
              bagging_fraction = 0.6,
              min_data_in_leaf = 3,
              num_rounds= 10000)



prepared_features <- lgb.prepare(data= model_data_df %>% select(dummyvars )  ) %>% # convertir character en nombre
  as.matrix(with = FALSE) # # Data input to LightGBM must be a matrix, without the label


dtrain <- lgb.Dataset(data= prepared_features, 
                      label = model_data_df$for_goal)

setinfo(dtrain, "init_score", log(model_data_df$duration))

cv_coll <- lgb.cv(param,
                  dtrain,
                  nfold = 5,
                  eval = "poisson",
                  early_stopping_rounds = 50)

model_nhl <- lgb.train(param,
                       dtrain,
                       nrounds= cv_coll$best_iter)



lgb.save(model_nhl, here::here("model_nhl20182019.model"))
model_nhl <- lgb.load( here::here("model_nhl20182019.model"))


temp <- data.frame(diag(2006)) 
dummyvars[1]
colnames(temp) <- colnames(model_data_df%>% select(dummyvars ))
temp <- bind_rows(temp %>% head(1) %>% mutate(against_players_8471679 = 0 ),
                  temp)





matrix_for_preds <- Matrix( lgb.prepare(data= temp  ) %>% # convertir character en nombre
                              as.matrix(with = FALSE),sparse = TRUE)
#tout_a_zero <- model_data_df %>% select(dummyvars )   %>% mutate_all(~ 0) # plante
preds <- tibble(preds =
                  predict(model_nhl,
                          matrix_for_preds)
                  )


preds$var <- c("ref", dummyvars)
preds <- preds %>% mutate(var = str_replace(var,"_players", ""))
preds2 <- preds %>% 
  separate(var, into = c("type", "player.id"))  %>% 
  mutate(player.id = as.integer(player.id)) %>%
  mutate(type =case_when(type == "for"~ "offense",
                         type == "against"~ "defense",
                         TRUE ~ type)
  )

players_data <- readRDS("players_data.rds")

reference <- preds2 %>% filter(type == "ref")  %>% pull(preds) %>% .[1]
reference * 3600
z <- preds2 %>% filter(type != "ref") %>% group_by(player.id) %>% spread(key= type, value = preds)


zz <- z %>% left_join(
  players_data %>% 
    select(player.id, player.fullName, player.primaryPosition.code, player.currentTeam.name)) %>%
  left_join(total_time_played %>% 
              rename(player.id = playerId) %>%
              mutate(hours_played = duration / 3600) %>% 
              select(-duration)) %>%
  mutate(
    offense = offense  * 3600,
    defense = defense * 3600,
    differential = offense - defense) %>%
  arrange(-differential) %>%
  filter(hours_played > 10)


offensive <- preds2 %>% 
  filter(type=="for") %>% 
  arrange(-preds) %>% 
  left_join(
    players_data %>% 
      select(player.id, player.fullName, player.primaryPosition.code, player.currentTeam.name)) %>%
  left_join(total_time_played %>% 
              rename(player.id = playerId) %>%
              mutate(hours_played = duration / 3600) %>% 
              select(-duration)) %>%
  filter(hours_played > 10)

defensive <- preds2 %>% 
  filter(type=="against") %>% 
  arrange(preds) %>% 
  left_join(
    players_data %>% 
      select(player.id, player.fullName, player.primaryPosition.code, player.currentTeam.name)) %>%
  left_join(total_time_played %>% 
              rename(player.id = playerId) %>%
              mutate(hours_played = duration / 3600) %>% 
              select(-duration)) %>%
  filter(hours_played > 10)

# 
# tout_a_zero <- model_data_df %>% 
#   select(dummyvars ) %>% 
#   mutate_all(~ 0)
# 
# model_nhl <- lgb.load( here::here("model_nhl.model"))
# preds <- tibble(preds_avec_subban = 
#                   predict(model_nhl,  
#                           Matrix(
#                             lgb.prepare(
#                               data= model_data_df %>% 
#                                 select(dummyvars ) %>% 
#                                 mutate_all(0) %>%
#                                 mutate(for_players_8474056==1)) %>%
#                               as.matrix(with = FALSE), 
#                             sparse = TRUE 
#                             
#                           )
#                   )
# )
# 
# 
# z <- model_data_df %>% 
#   select(dummyvars ) %>% 
#   mutate_all(~ 0)


#%>%
# mutate(for_players_8474056==1))

## fin lightgbm

# parglm (multicore?)
library(parglm)

myglm <- parglm(formula = fla,
                data = model_data_df,
                family = poisson(link = log),
                control = parglm.control(nthreads = 20L))

# speedglm (singlecore)
write_rds(model_data_df,"model_data_df.rds" )
write_rds(fla, "fla.rds")

model_data_df <- read_rds("model_data_df.rds" )
fla <- read_rds( "fla.rds")
tdeb <- Sys.time()
mod.glm <- speedglm(formula = fla,
                    data = model_data_df,
                    family = poisson(link = log))

tfin <- Sys.time()
as.numeric(tfin - tdeb, units = "mins") # 3 minutes # crash 30 GB.

#mod.glm$coefficients
saveRDS(mod.glm, file = "mod.glm.rds")
coefs <- broom::tidy(mod.glm)
#broom::glance(mod.glm)

# best defensive contributors
defense <- coefs  %>%  filter(str_detect(term, "against")) %>% arrange(estimate) %>% mutate(playerId = as.numeric(str_extract(term, "\\d+"))) %>% left_join(
  players_data %>% select(
    playerId = player.id,
    player.fullName,
    player.primaryPosition.name,
    player.currentTeam.name
  )
)  %>% left_join(total_time_played ) %>% filter(duration> 10000) %>% select(-term) 


# best offensive contributors
attack <- coefs  %>%  filter(str_detect(term, "for")) %>% 
  arrange(-estimate) %>% 
  mutate(playerId = as.numeric(str_extract(term, "\\d+"))) %>% 
  left_join(
    players_data %>% select(
      playerId = player.id,
      player.fullName,
      player.primaryPosition.name,
      player.currentTeam.name
    )
  )  %>% left_join(total_time_played ) %>% filter(duration> 10000) %>% select(-term) 

