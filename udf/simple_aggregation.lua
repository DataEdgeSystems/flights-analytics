
local function add_values(airlineMap, nextFlight)
  local carrier = nextFlight["CARRIER"]
  local airline = airlineMap[carrier] 
  if airline == null then
    airline = map {flights = 0, late = 0}
  end
  airline.flights = airline.flights + 1
  -- if this flight is late, increment the late count in airline
  if nextFlight.ELAPSED_TIME > (nextFlight.ARR_TIME - nextFlight.DEP_TIME) then
    airline.late = airline.late + 1
  end
  -- put the airline into the airlineMap
  airlineMap[carrier] = airline
  return airlineMap
end

local function flightsMerge(a, b)
  print("type a: "..tostring(type(a)))
  print("type b: "..tostring(type(b)))
  a.flights = a.flights + b.flights
  a.late = a.late + b.late
  return a
end

local function reduce_values(a, b)
  print("map a: "..tostring(map.size(a)))
  print("map b: "..tostring(map.size(b)))
  return map.merge(a, b, flightsMerge)
  --return a
end

function late_flights_by_airline(stream)

  return stream : aggregate(map(), add_values) 
    : reduce(reduce_values)

end


