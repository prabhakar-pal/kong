local timestamp = require "kong.tools.timestamp"
local Errors = require "kong.dao.errors"
local BaseDB = require "kong.dao.base_db"
local utils = require "kong.tools.utils"
local uuid = utils.uuid
local json = require "cjson"

local mongorover = require "mongorover"

local function getTableFromCollection(collection)
  local  res_rows = {}
  local count = 0
 
  for row in collection do
      res_rows[#res_rows + 1] = row
      count = count + 1
  end
  return res_rows
end

local function table_length(table1)
    local count = 0
     
    for _,row in ipairs(table1) do
      for k,v in pairs(row) do
	count = count + 1
      end
    end
    return count
end
local function copy_table(table)
    rettable = {}
    for k, v in pairs(table) do
        rettable[k] = v
    end
    return rettable
end

local MongoDB = BaseDB:extend()

MongoDB.dao_insert_values = {
  id = function()
    return uuid()
  end,
  timestamp = function()
    return timestamp.get_utc()
  end
}

function MongoDB:new(kong_config)
  local conn_opts = {
    shm = "MongoDB",
    prepared_shm = "MongoDB_prepared",
    contact_points = kong_config.mongo_contact_points,
    keyspace = kong_config.mongo_keyspace,
    protocol_options = {
      default_port = kong_config.mongo_port
    }
  }

  self.mongo_client = mongorover.MongoClient.new("mongodb://"..tostring(kong_config.mongo_contact_points[1])..":"..tostring(kong_config.mongo_port))
  self.mongo_database = self.mongo_client:getDatabase(kong_config.mongo_database)

  MongoDB.super.new(self, "MongoDB", conn_opts)
end

function MongoDB:infos()
  return {
    desc = "keyspace",
    name = self:_get_conn_options().keyspace
  }
end

--- Querying
local function check_unique_constraints(self, table_name, constraints, values, primary_keys, update)

  local errors
  mongo_collection = self.mongo_database:getCollection(table_name)
  for col, constraint in pairs(constraints.unique) do
    -- Only check constraints if value is non-null
    if values[col] ~= nil then
      criterion = {}
      criterion[col] = values[col]
      local rows = mongo_collection:find(criterion)
  
      local le = 0
      for row in rows do
        for k,v in pairs(row) do
            le = le + 1
        end
      end

      if (le > 0) then
          errors = utils.add_error(errors, col, values[col])
      end
    end
  end
  return Errors.unique(errors)
end

local function check_foreign_constaints(self, values, constraints)
  local errors

  for col, constraint in pairs(constraints.foreign) do
    -- Only check foreign keys if value is non-null, if must not be null, field should be required
    if values[col] ~= nil then
      local res, err = self:find(constraint.table, constraint.schema, {[constraint.col] = values[col]})
      if err then
        return err
      elseif res == nil then
        errors = utils.add_error(errors, col, values[col])
      end
    end
  end

  return Errors.foreign(errors)
end

function MongoDB:query(query, args, opts, schema, no_keyspace)
  -- TODO
  return {}
end

function MongoDB:insert(table_name, schema, model, constraints, options)

  local err = check_unique_constraints(self, table_name, constraints, model)
  if err then
    return nil, err
  end

  model1 = copy_table(model)
  mongo_collection = self.mongo_database:getCollection(table_name)
  mongo_collection:insert_one(model1)
  return model
end

function MongoDB:find(table_name, schema, filter_keys)
  mongo_collection = self.mongo_database:getCollection(table_name)
  local rows = mongo_collection:find(filter_keys)
  local rows_as_table = getTableFromCollection(rows)
  if rows_as_table then
      return rows_as_table[1]
  end
end

function MongoDB:find_all(table_name, key, schema)

  mongo_collection = self.mongo_database:getCollection(table_name)
  if key == nil then
    key = {}
  end
  local rows = mongo_collection:find(key)
  local rows_as_table = getTableFromCollection(rows)
  return rows_as_table
end

function MongoDB:find_page(table_name, tbl, paging_state, page_size, schema)
  local rows = self:find_all(table_name,tbl,schema)
  return rows, nil, nil
end

function MongoDB:count(table_name, tbl, schema)

  local rows,_ = self:find_all(table_name,tbl,schema)
  local le = table_length(rows)
  return le
end

function MongoDB:update(table_name, schema, constraints, filter_keys, values, nils, full, model, options)
  -- must check unique constaints manually too
  -- TODO: Update
  return self:find(table_name, schema, filter_keys)
end

function MongoDB:delete(table_name, schema, primary_keys, constraints)

  local row, err = self:find(table_name, schema, primary_keys)
  if err or row == nil then
    return nil, err
  end

  mongo_collection = self.mongo_database:getCollection(table_name)
  mongo_collection:delete_one(primary_keys)
  return row
end

-- Migrations

function MongoDB:queries(queries, no_keyspace)
  return nil
end

function MongoDB:drop_table(table_name)
  -- TODO
  return select(2, self:query("DROP TABLE "..table_name))
end

function MongoDB:truncate_table(table_name)
  -- TODO
  return select(2, self:query("TRUNCATE "..table_name))
end

function MongoDB:current_migrations()
  -- TODO
  return {}
end

function MongoDB:record_migration(id, name)
  -- return {}
end

return MongoDB
