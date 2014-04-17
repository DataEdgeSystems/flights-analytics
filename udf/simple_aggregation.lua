local function toMinutes(time)
  local hours = math.floor(time / 100)
  local minutes = time%100
  return hours * 60 + minutes

end

local function add_values(airlineMap, nextFlight)
  local carrier = nextFlight["CARRIER"]
  local airline = airlineMap[carrier] 
  if airline == null then
    airline = map {flights = 0, late = 0}
  end
  airline.flights = airline.flights + 1
  -- if this flight is late, increment the late count in airline
  if toMinutes(nextFlight.ELAPSED_TIME) > (toMinutes(nextFlight.ARR_TIME) - toMinutes(nextFlight.DEP_TIME)) then
    airline.late = airline.late + 1
  end
  -- put the airline into the airlineMap
  airlineMap[carrier] = airline
  return airlineMap
end

local function flightsMerge(a, b)
  a.flights = a.flights + b.flights
  a.late = a.late + b.late
  a.percent = a.late / a.flights * 100 
  return a
end

local function reduce_values(a, b)
  return map.merge(a, b, flightsMerge)
  --return a
end

function late_flights_by_airline(stream)

  return stream : aggregate(map(), add_values) 
    : reduce(reduce_values)

end


