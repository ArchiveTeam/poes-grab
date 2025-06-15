local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_type = nil
local item_name = nil
local item_value = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs({
    ["^https?://www%.ncei%.noaa%.gov/data/(.+)$"]="ncei-data-file",
    ["^https?://(noaa%-cdr%-polar%-pathfinder%-fcdr%-pds%.s3%.amazonaws%.com/data/.+)$"]="ncei-data-file-aws"
  }) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {}
    new_item_type = found["type"]
    new_item_value = found["value"]
    if new_item_type == "ncei-data-file-aws" then
      local a, b = string.match(new_item_value, "^(noaa%-cdr%-polar%-pathfinder%-fcdr%-pds%.s3%.amazonaws%.com/data/)(.+)$")
      if a and b then
        new_item_value = "avhrr-polar-pathfinder/access/" .. b
      end
    end
    new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name then
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  return true
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}

  if abortgrab then
    return {}
  end

  local function check(newurl)
    if not processed(newurl)
      and allowed(newurl, origurl) then
      table.insert(urls, {
        url=newurl
      })
      addedtolist[newurl] = true
    end
  end

  for k, v in pairs({
    ["/data/avhrr%-hirs%-reflectance%-and%-cloud%-properties%-patmosx/access/(.+[^/])$"]="https://noaa-cdr-patmosx-radiances-and-clouds-pds.s3.amazonaws.com/data/",
    ["/data/avhrr%-polar%-pathfinder/access/(.+[^/])$"]="https://noaa-cdr-polar-pathfinder-fcdr-pds.s3.amazonaws.com/data/",
    ["/data/amsu%-a%-brightness%-temperature/access/(.+[^/])$"]="https://noaa-cdr-microwave-temp-sounder-brit-temp-pds.s3.amazonaws.com/data/",
    ["/data/amsu%-a%-brightness%-temperature%-noaa/access/(.+[^/])$"]="https://noaa-cdr-microwave-brit-temp-pds.s3.amazonaws.com/data/",
    ["/data/amsu%-b%-mhs%-brightness%-temperature/access/(.+[^/])$"]="https://noaa-cdr-microwave-humidity-sounder-brit-temp-pds.s3.amazonaws.com/data/",
    ["/data/avhrr%-polar%-pathfinder%-extended/access/(.+[^/])$"]="https://noaa-cdr-polar-pathfinder-extended-pds.s3.amazonaws.com/data/",
    ["/data/avhrr%-aerosol%-optical%-thickness/access/(.+[^/])$"]="https://noaa-cdr-aerosol-optical-thickness-pds.s3.amazonaws.com/data/",
    ["/data/land%-normalized%-difference%-vegetation%-index/access/(.+[^/])$"]="https://noaa-cdr-ndvi-pds.s3.amazonaws.com/data/",
    ["/data/land%-leaf%-area%-index%-and%-fapar/access/(.+[^/])$"]="https://noaa-cdr-leaf-area-index-fapar-pds.s3.amazonaws.com/data/",
    ["/data/land%-surface%-reflectance/access/(.+[^/])$"]="https://noaa-cdr-surface-reflectance-polar-orbiter-pds.s3.amazonaws.com/data/",
    ["/data/mean%-layer%-temperature%-ucar%-lower%-stratosphere/access/(.+[^/])$"]="https://noaa-cdr-mean-layer-temp-lower-strat-pds.s3.amazonaws.com/data/",
    ["/data/mean%-layer%-temperature%-noaa/access/(.+[^/])$"]="https://noaa-cdr-mean-layer-temp-pds.s3.amazonaws.com/data/",
    ["/data/mean%-layer%-temperature%-ucar%-upper%-trop%-lower%-strat/access/(.+[^/])$"]="https://noaa-cdr-mean-layer-temp-upper-trop-lower-strat-pds.s3.amazonaws.com/data/",
    ["/data/hydrological%-properties/access/(.+[^/])$"]="https://noaa-cdr-hydrological-properties-pds.s3.amazonaws.com/data/",
  }) do
    local path = string.match(url, k)
    if path then
      check(v .. path)
    end
  end
 
  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200 then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 8
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    downloaded[url["url"]] = true
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  if item_type == "ncei-data-file"
    and not string.match(item_value, "^avhrr%-reflectance%-cloud%-properties%-patmos%-extended/")
    and not string.match(item_value, "oceans/pathfinder/")
    and not string.match(item_value, "^poes%-metop%-space%-environment%-monitor/")
    and total_downloaded_bytes > 1 * (1024 ^ 2) then
    local warc_file = io.open(item_dir .. "/" .. warc_file_base .. ".warc.gz", "r")
    local warc_size = warc_file:seek("end")
    if warc_size > total_downloaded_bytes * 0.51 then
      kill_grab()
    end
  end
  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


