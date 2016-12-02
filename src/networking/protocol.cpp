//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// protocol.cpp
//
// Identification: src/wire/protocol.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <unordered_map>
#include "common/cache.h"
#include "common/macros.h"
#include "common/portal.h"
#include "common/types.h"
#include "tcop/tcop.h"
#include "wire/marshal.h"
#include "wire/wire.h"

#include "common/value.h"
#include "common/value_factory.h"
#include "planner/abstract_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"

#include <boost/algorithm/string.hpp>

#define PROTO_MAJOR_VERSION(x) x >> 16

namespace peloton {
namespace wire {

// TODO: Remove hardcoded auth strings
// Hardcoded authentication strings used during session startup. To be removed
const std::unordered_map<std::string, std::string>
    PacketManager::parameter_status_map_ =
        boost::assign::map_list_of("application_name", "psql")(
            "client_encoding", "UTF8")("DateStyle", "ISO, MDY")(
            "integer_datetimes", "on")("IntervalStyle", "postgres")(
            "is_superuser", "on")("server_encoding", "UTF8")(
            "server_version", "9.5devel")("session_authorization", "postgres")(
            "standard_conforming_strings", "on")("TimeZone", "US/Eastern");

void PacketManager::MakeHardcodedParameterStatus(
    const std::pair<std::string, std::string> &kv) {
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = PARAMETER_STATUS;
  PacketPutString(response.get(), kv.first);
  PacketPutString(response.get(), kv.second);
  responses.push_back(std::move(response));
}
/*
 * process_startup_packet - Processes the startup packet
 *  (after the size field of the header).
 */
bool PacketManager::ProcessStartupPacket(InputPacket *pkt) {
  std::string token, value;
  std::unique_ptr<OutputPacket> response(new OutputPacket());

  int32_t proto_version = PacketGetInt(pkt, sizeof(int32_t));

  // Only protocol version 3 is supported
  if (PROTO_MAJOR_VERSION(proto_version) != 3) {
    LOG_ERROR("Protocol error: Only protocol version 3 is supported but received %d", PROTO_MAJOR_VERSION(proto_version));
    exit(EXIT_FAILURE);
  }

  // TODO: check for more malformed cases
  // iterate till the end
  for (;;) {
    // loop end case?
    if (pkt->ptr >= pkt->len) break;
    GetStringToken(pkt, token);

    // if the option database was found
    if (token.compare("database") == 0) {
      // loop end?
      if (pkt->ptr >= pkt->len) break;
      GetStringToken(pkt, client_.dbname);
    } else if (token.compare(("user")) == 0) {
      // loop end?
      if (pkt->ptr >= pkt->len) break;
      GetStringToken(pkt, client_.user);
    } else {
      if (pkt->ptr >= pkt->len) break;
      GetStringToken(pkt, value);
      client_.cmdline_options[token] = value;
    }
  }

  // send auth-ok ('R')
  response->msg_type = AUTHENTICATION_REQUEST;
  PacketPutInt(response.get(), 0, 4);
  responses.push_back(std::move(response));

  // Send the parameterStatus map ('S')
  for (auto it = parameter_status_map_.begin();
       it != parameter_status_map_.end(); it++) {
    MakeHardcodedParameterStatus(*it);
  }

  // ready-for-query packet -> 'Z'
  SendReadyForQuery(TXN_IDLE);

  // we need to send the response right away
  force_flush = true;

  return true;
}

void PacketManager::PutTupleDescriptor(
    const std::vector<FieldInfoType> &tuple_descriptor) {
  if (tuple_descriptor.empty()) return;

  std::unique_ptr<OutputPacket> pkt(new OutputPacket());
  pkt->msg_type = ROW_DESCRIPTION;
  PacketPutInt(pkt.get(), tuple_descriptor.size(), 2);

  for (auto col : tuple_descriptor) {
    PacketPutString(pkt.get(), std::get<0>(col));
    // TODO: Table Oid (int32)
    PacketPutInt(pkt.get(), 0, 4);
    // TODO: Attr id of column (int16)
    PacketPutInt(pkt.get(), 0, 2);
    // Field data type (int32)
    PacketPutInt(pkt.get(), std::get<1>(col), 4);
    // Data type size (int16)
    PacketPutInt(pkt.get(), std::get<2>(col), 2);
    // Type modifier (int32)
    PacketPutInt(pkt.get(), -1, 4);
    // Format code for text
    PacketPutInt(pkt.get(), 0, 2);
  }
  responses.push_back(std::move(pkt));
}

void PacketManager::SendDataRows(std::vector<ResultType> &results, int colcount,
                                 int &rows_affected) {
  if (results.empty() || colcount == 0) return;

  size_t numrows = results.size() / colcount;

  // 1 packet per row
  for (size_t i = 0; i < numrows; i++) {
    std::unique_ptr<OutputPacket> pkt(new OutputPacket());
    pkt->msg_type = DATA_ROW;
    PacketPutInt(pkt.get(), colcount, 2);
    for (int j = 0; j < colcount; j++) {
      // length of the row attribute
      PacketPutInt(pkt.get(), results[i * colcount + j].second.size(), 4);
      // contents of the row attribute
      PacketPutBytes(pkt.get(), results[i * colcount + j].second);
    }
    responses.push_back(std::move(pkt));
  }
  rows_affected = numrows;
}

/* Gets the first token of a query */
std::string get_query_type(std::string query) {
  std::string query_type;
  std::stringstream stream(query);
  stream >> query_type;
  return query_type;
}

void PacketManager::CompleteCommand(const std::string &query_type, int rows) {
  std::unique_ptr<OutputPacket> pkt(new OutputPacket());
  pkt->msg_type = COMMAND_COMPLETE;
  std::string tag = query_type;
  /* After Begin, we enter a txn block */
  if (query_type.compare("BEGIN") == 0) txn_state_ = TXN_BLOCK;
  /* After commit, we end the txn block */
  else if (query_type.compare("COMMIT") == 0)
    txn_state_ = TXN_IDLE;
  /* After rollback, the txn block is ended */
  else if (!query_type.compare("ROLLBACK"))
    txn_state_ = TXN_IDLE;
  /* the rest are custom status messages for each command */
  else if (!query_type.compare("INSERT"))
    tag += " 0 " + std::to_string(rows);
  else
    tag += " " + std::to_string(rows);
  PacketPutString(pkt.get(), tag);

  responses.push_back(std::move(pkt));
}

/*
 * put_empty_query_response - Informs the client that an empty query was sent
 */
void PacketManager::SendEmptyQueryResponse() {
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = EMPTY_QUERY_RESPONSE;
  responses.push_back(std::move(response));
}

bool PacketManager::HardcodedExecuteFilter(std::string query_type) {
  // Skip SET
  if (query_type.compare("SET") == 0 || query_type.compare("SHOW") == 0)
    return false;
  // skip duplicate BEGIN
  if (!query_type.compare("BEGIN") && txn_state_ == TXN_BLOCK) return false;
  // skip duplicate Commits
  if (!query_type.compare("COMMIT") && txn_state_ == TXN_IDLE) return false;
  // skip duplicate Rollbacks
  if (!query_type.compare("ROLLBACK") && txn_state_ == TXN_IDLE) return false;
  return true;
}

// The Simple Query Protocol
void PacketManager::ExecQueryMessage(InputPacket *pkt) {
  std::string q_str;
  PacketGetString(pkt, pkt->len, q_str);

  std::vector<std::string> queries;
  boost::split(queries, q_str, boost::is_any_of(";"));

  if (queries.size() == 1) {
    SendEmptyQueryResponse();
    SendReadyForQuery(txn_state_);
    return;
  }

  // Get traffic cop
  auto &tcop = tcop::TrafficCop::GetInstance();

  for (auto query : queries) {
    // iterate till before the empty string after the last ';'
    if (query != queries.back()) {
      if (query.empty()) {
        SendEmptyQueryResponse();
        SendReadyForQuery(TXN_IDLE);
        return;
      }

      std::vector<ResultType> result;
      std::vector<FieldInfoType> tuple_descriptor;
      std::string error_message;
      int rows_affected;

      // execute the query using tcop
      auto status = tcop.ExecuteStatement(query, result, tuple_descriptor,
                                          rows_affected, error_message);

      // check status
      if (status == Result::RESULT_FAILURE) {
        SendErrorResponse({{HUMAN_READABLE_ERROR, error_message}});
        break;
      }

      // send the attribute names
      PutTupleDescriptor(tuple_descriptor);

      // send the result rows
      SendDataRows(result, tuple_descriptor.size(), rows_affected);

      // TODO: should change to query_type
      CompleteCommand(query, rows_affected);
    } else if (queries.size() == 1) {
      // just a ';' sent
      SendEmptyQueryResponse();
    }
  }

  SendReadyForQuery(READ_FOR_QUERY);
}

/*
 * exec_parse_message - handle PARSE message
 */
void PacketManager::ExecParseMessage(InputPacket *pkt) {
  std::string error_message, statement_name, query_string, query_type;
  GetStringToken(pkt, statement_name);

  // Read prepare statement name
  // Read query string
  GetStringToken(pkt, query_string);
  skipped_stmt_ = false;
  query_type = get_query_type(query_string);

  // For an empty query or a query to be filtered, just send parse complete
  // response and don't execute
  if (query_string == "" || HardcodedExecuteFilter(query_type) == false) {
    skipped_stmt_ = true;
    skipped_query_string_ = std::move(query_string);
    skipped_query_type_ = std::move(query_type);

    // Send Parse complete response
    std::unique_ptr<OutputPacket> response(new OutputPacket());
    response->msg_type = PARSE_COMPLETE;
    responses.push_back(std::move(response));
    return;
  }

  // Prepare statement
  std::shared_ptr<Statement> statement(nullptr);
  auto &tcop = tcop::TrafficCop::GetInstance();

  statement =
      tcop.PrepareStatement(statement_name, query_string, error_message);
  if (statement.get() == nullptr) {
    skipped_stmt_ = true;
    SendErrorResponse({{HUMAN_READABLE_ERROR, error_message}});
    LOG_TRACE("ExecParse Error");
    return;
  }

  // Read number of params
  int num_params = PacketGetInt(pkt, 2);

  // Read param types
  std::vector<int32_t> param_types(num_params);
  auto type_buf_begin = pkt->Begin() + pkt->ptr;
  auto type_buf_len = ReadParamType(pkt, num_params, param_types);

  // Cache the received query
  bool unnamed_query = statement_name.empty();
  statement->SetQueryType(query_type);
  statement->SetParamTypes(param_types);

  // Stat
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    // Make a copy of param types for stat collection
    stats::QueryMetric::QueryParamBuf query_type_buf;
    query_type_buf.len = type_buf_len;
    query_type_buf.buf = PacketCopyBytes(type_buf_begin, type_buf_len);

    // Unnamed statement
    if (unnamed_query) {
      unnamed_stmt_param_types_ = query_type_buf;
    } else {
      statement_param_types_[statement_name] = query_type_buf;
    }
  }

  // Unnamed statement
  if (unnamed_query) {
    unnamed_statement_ = statement;
  } else {
    auto entry = std::make_pair(statement_name, statement);
    statement_cache_.insert(entry);
  }
  // Send Parse complete response
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = PARSE_COMPLETE;
  responses.push_back(std::move(response));
}

void PacketManager::ExecBindMessage(InputPacket *pkt) {
  std::string portal_name, statement_name;
  // BIND message
  GetStringToken(pkt, portal_name);
  GetStringToken(pkt, statement_name);

  if (skipped_stmt_) {
    // send bind complete
    std::unique_ptr<OutputPacket> response(new OutputPacket());
    response->msg_type = BIND_COMPLETE;
    responses.push_back(std::move(response));
    return;
  }

  // Read parameter format
  int num_params_format = PacketGetInt(pkt, 2);
  std::vector<int16_t> formats(num_params_format);

  auto format_buf_begin = pkt->Begin() + pkt->ptr;
  auto format_buf_len = ReadParamFormat(pkt, num_params_format, formats);

  int num_params = PacketGetInt(pkt, 2);
  // error handling
  if (num_params_format != num_params) {
    std::string error_message =
        "Malformed request: num_params_format is not equal to num_params";
    SendErrorResponse({{HUMAN_READABLE_ERROR, error_message}});
    return;
  }

  // Get statement info generated in PARSE message
  std::shared_ptr<Statement> statement;
  stats::QueryMetric::QueryParamBuf param_type_buf;

  if (statement_name.empty()) {
    statement = unnamed_statement_;
    param_type_buf = unnamed_stmt_param_types_;

    // Check unnamed statement
    if (statement.get() == nullptr) {
      std::string error_message = "Invalid unnamed statement";
      LOG_ERROR("%s", error_message.c_str());
      SendErrorResponse({{HUMAN_READABLE_ERROR, error_message}});
      return;
    }
  } else {
    auto statement_cache_itr = statement_cache_.find(statement_name);
    // Did not find statement with same name
    if (statement_cache_itr != statement_cache_.end()) {
      statement = *statement_cache_itr;
      param_type_buf = statement_param_types_[statement_name];
    }
    // Found statement with same name
    else {
      std::string error_message = "Prepared statement name already exists";
      LOG_ERROR("%s", error_message.c_str());
      SendErrorResponse({{HUMAN_READABLE_ERROR, error_message}});
      return;
    }
  }

  const auto &query_string = statement->GetQueryString();
  const auto &query_type = statement->GetQueryType();

  // check if the loaded statement needs to be skipped
  skipped_stmt_ = false;
  if (!HardcodedExecuteFilter(query_type)) {
    skipped_stmt_ = true;
    skipped_query_string_ = query_string;
    std::unique_ptr<OutputPacket> response(new OutputPacket());
    // Send Bind complete response
    response->msg_type = BIND_COMPLETE;
    responses.push_back(std::move(response));
    return;
  }

  // Group the parameter types and the parameters in this vector
  std::vector<std::pair<int, std::string>> bind_parameters(num_params);
  std::vector<common::Value> param_values(num_params);

  auto param_types = statement->GetParamTypes();

  auto val_buf_begin = pkt->Begin() + pkt->ptr;
  auto val_buf_len = ReadParamValue(pkt, num_params, param_types,
                                    bind_parameters, param_values, formats);

  int format_codes_number = PacketGetInt(pkt, 2);
  LOG_TRACE("format_codes_number: %d", format_codes_number);
  // Set the result-column format code
  if (format_codes_number == 0) {
    // using the default text format
    result_format_ =
        std::move(std::vector<int>(statement->GetTupleDescriptor().size(), 0));
  } else if (format_codes_number == 1) {
    // get the format code from packet
    auto result_format = PacketGetInt(pkt, 2);
    result_format_ = std::move(std::vector<int>(
        statement->GetTupleDescriptor().size(), result_format));
  } else {
    // get the format code for each column
    result_format_.clear();
    for (int format_code_idx = 0; format_code_idx < format_codes_number;
         ++format_code_idx) {
      result_format_.push_back(PacketGetInt(pkt, 2));
      LOG_TRACE("format code: %d", *result_format_.rbegin());
    }
  }

  if (param_values.size() > 0) {
    statement->GetPlanTree()->SetParameterValues(&param_values);
    // Instead of tree traversal, we should put param values in the
    // executor context.
  }

  std::shared_ptr<stats::QueryMetric::QueryParams> param_stat(nullptr);
  if (FLAGS_stats_mode != STATS_TYPE_INVALID && num_params > 0) {
    // Make a copy of format for stat collection
    stats::QueryMetric::QueryParamBuf param_format_buf;
    param_format_buf.len = format_buf_len;
    param_format_buf.buf = PacketCopyBytes(format_buf_begin, format_buf_len);
    PL_ASSERT(format_buf_len > 0);

    // Make a copy of value for stat collection
    stats::QueryMetric::QueryParamBuf param_val_buf;
    param_val_buf.len = val_buf_len;
    param_val_buf.buf = PacketCopyBytes(val_buf_begin, val_buf_len);
    PL_ASSERT(val_buf_len > 0);

    param_stat.reset(new stats::QueryMetric::QueryParams(
        param_format_buf, param_type_buf, param_val_buf, num_params));
  }

  // Construct a portal.
  // Notice that this will move param_values so no value will be left there.
  auto portal =
      new Portal(portal_name, statement, std::move(param_values), param_stat);
  std::shared_ptr<Portal> portal_reference(portal);

  auto itr = portals_.find(portal_name);
  // Found portal name in portal map
  if (itr != portals_.end()) {
    itr->second = portal_reference;
  }
  // Create a new entry in portal map
  else {
    portals_.insert(std::make_pair(portal_name, portal_reference));
  }
  // send bind complete
  std::unique_ptr<OutputPacket> response(new OutputPacket());
  response->msg_type = BIND_COMPLETE;
  responses.push_back(std::move(response));
}

size_t PacketManager::ReadParamType(InputPacket *pkt, int num_params,
                                    std::vector<int32_t> &param_types) {
  auto begin = pkt->ptr;
  // get the type of each parameter
  for (int i = 0; i < num_params; i++) {
    int param_type = PacketGetInt(pkt, 4);
    param_types[i] = param_type;
  }
  auto end = pkt->ptr;
  return end - begin;
}

size_t PacketManager::ReadParamFormat(InputPacket *pkt, int num_params_format,
                                      std::vector<int16_t> &formats) {
  auto begin = pkt->ptr;
  // get the format of each parameter
  for (int i = 0; i < num_params_format; i++) {
    formats[i] = PacketGetInt(pkt, 2);
  }
  auto end = pkt->ptr;
  return end - begin;
}

// For consistency, this function assumes the input vectors has the correct size
size_t PacketManager::ReadParamValue(
    InputPacket *pkt, int num_params, std::vector<int32_t> &param_types,
    std::vector<std::pair<int, std::string>> &bind_parameters,
    std::vector<common::Value> &param_values, std::vector<int16_t> &formats) {

  auto begin = pkt->ptr;
  ByteBuf param;
  for (int param_idx = 0; param_idx < num_params; param_idx++) {
    int param_len = PacketGetInt(pkt, 4);
    // BIND packet NULL parameter case
    if (param_len == -1) {
      // NULL mode
      auto peloton_type = PostgresValueTypeToPelotonValueType(
          static_cast<PostgresValueType>(param_types[param_idx]));
      bind_parameters[param_idx] =
          std::make_pair(peloton_type, std::string(""));
      param_values[param_idx] =
          common::ValueFactory::GetNullValueByType(peloton_type);
    } else {
      PacketGetBytes(pkt, param_len, param);

      if (formats[param_idx] == 0) {
        // TEXT mode
        std::string param_str = std::string(std::begin(param), std::end(param));
        bind_parameters[param_idx] =
            std::make_pair(common::Type::VARCHAR, param_str);
        if (PostgresValueTypeToPelotonValueType(
                (PostgresValueType)param_types[param_idx]) ==
            common::Type::VARCHAR) {
          param_values[param_idx] =
              common::ValueFactory::GetVarcharValue(param_str);
        } else {
          param_values[param_idx] =
              (common::ValueFactory::GetVarcharValue(param_str))
                  .CastAs(PostgresValueTypeToPelotonValueType(
                       (PostgresValueType)param_types[param_idx]));
        }
        PL_ASSERT(param_values[param_idx].GetTypeId() != common::Type::INVALID);
      } else {
        // BINARY mode
        switch (param_types[param_idx]) {
          case POSTGRES_VALUE_TYPE_INTEGER: {
            int int_val = 0;
            for (size_t i = 0; i < sizeof(int); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters[param_idx] =
                std::make_pair(common::Type::INTEGER, std::to_string(int_val));
            param_values[param_idx] =
                common::ValueFactory::GetIntegerValue(int_val).Copy();
          } break;
          case POSTGRES_VALUE_TYPE_BIGINT: {
            int64_t int_val = 0;
            for (size_t i = 0; i < sizeof(int64_t); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters[param_idx] =
                std::make_pair(common::Type::BIGINT, std::to_string(int_val));
            param_values[param_idx] =
                common::ValueFactory::GetBigIntValue(int_val).Copy();
          } break;
          case POSTGRES_VALUE_TYPE_DOUBLE: {
            double float_val = 0;
            unsigned long buf = 0;
            for (size_t i = 0; i < sizeof(double); ++i) {
              buf = (buf << 8) | param[i];
            }
            PL_MEMCPY(&float_val, &buf, sizeof(double));
            bind_parameters[param_idx] = std::make_pair(
                common::Type::DECIMAL, std::to_string(float_val));
            param_values[param_idx] =
                common::ValueFactory::GetDoubleValue(float_val).Copy();
          } break;
          case POSTGRES_VALUE_TYPE_VARBINARY: {
            bind_parameters[param_idx] = std::make_pair(
                common::Type::VARBINARY,
                std::string(reinterpret_cast<char *>(&param[0]), param_len));
            param_values[param_idx] = common::ValueFactory::GetVarbinaryValue(
                &param[0], param_len).Copy();
          } break;
          default: {
            LOG_ERROR("Do not support data type: %d", param_types[param_idx]);
          } break;
        }
        PL_ASSERT(param_values[param_idx].GetTypeId() != common::Type::INVALID);
      }
    }
  }
  auto end = pkt->ptr;
  return end - begin;
}

bool PacketManager::ExecDescribeMessage(InputPacket *pkt) {
  if (skipped_stmt_) {
    // send 'no-data' message
    std::unique_ptr<OutputPacket> response(new OutputPacket());
    response->msg_type = NO_DATA_RESPONSE;
    responses.push_back(std::move(response));
    return true;
  }

  ByteBuf mode;
  std::string portal_name;
  PacketGetBytes(pkt, 1, mode);
  GetStringToken(pkt, portal_name);
  if (mode[0] == 'P') {
    LOG_TRACE("Describe a portal");
    auto portal_itr = portals_.find(portal_name);

    // TODO: error handling here
    // Ahmed: This is causing the continuously running thread
    // Changed the function signature to return boolean
    // when false is returned, the connection is closed
    if (portal_itr == portals_.end()) {
      LOG_ERROR("Did not find portal : %s", portal_name.c_str());
      std::vector<FieldInfoType> tuple_descriptor;
      PutTupleDescriptor(tuple_descriptor);
      return false;
    }

    auto portal = portal_itr->second;
    if (portal == nullptr) {
      LOG_ERROR("Portal does not exist : %s", portal_name.c_str());
      std::vector<FieldInfoType> tuple_descriptor;
      PutTupleDescriptor(tuple_descriptor);
      return false;
    }

    auto statement = portal->GetStatement();
    PutTupleDescriptor(statement->GetTupleDescriptor());
  } else {
    LOG_TRACE("Describe a prepared statement");
  }
  return true;
}

void PacketManager::ExecExecuteMessage(InputPacket *pkt) {
  // EXECUTE message
  std::vector<ResultType> results;
  std::string error_message, portal_name;
  int rows_affected = 0;
  GetStringToken(pkt, portal_name);

  // covers weird JDBC edge case of sending double BEGIN statements. Don't
  // execute them
  if (skipped_stmt_) {
    if (skipped_query_string_ == "") {
      SendEmptyQueryResponse();
    } else {
      CompleteCommand(skipped_query_type_, rows_affected);
    }
    skipped_stmt_ = false;
    return;
  }

  auto portal = portals_[portal_name];
  if (portal.get() == nullptr) {
    LOG_ERROR("Did not find portal : %s", portal_name.c_str());
    SendErrorResponse({{HUMAN_READABLE_ERROR, error_message}});
    SendReadyForQuery(txn_state_);
    return;
  }

  auto statement = portal->GetStatement();
  const auto &query_type = statement->GetQueryType();

  auto param_stat = portal->GetParamStat();
  if (statement.get() == nullptr) {
    LOG_ERROR("Did not find statement in portal : %s", portal_name.c_str());
    SendErrorResponse({{HUMAN_READABLE_ERROR, error_message}});
    SendReadyForQuery(txn_state_);
    return;
  }

  auto statement_name = statement->GetStatementName();
  bool unnamed = statement_name.empty();
  auto param_values = portal->GetParameters();

  auto &tcop = tcop::TrafficCop::GetInstance();
  auto status = tcop.ExecuteStatement(statement, param_values, unnamed,
                                      param_stat, result_format_, results,
                                      rows_affected, error_message);

  if (status == Result::RESULT_FAILURE) {
    LOG_ERROR("Failed to execute: %s", error_message.c_str());
    SendErrorResponse({{HUMAN_READABLE_ERROR, error_message}});
    SendReadyForQuery(txn_state_);
  }
  // put_row_desc(portal->rowdesc);
  auto tuple_descriptor = statement->GetTupleDescriptor();
  SendDataRows(results, tuple_descriptor.size(), rows_affected);
  CompleteCommand(query_type, rows_affected);
}

/*
 * process_packet - Main switch block; process incoming packets,
 *  Returns false if the session needs to be closed.
 */
bool PacketManager::ProcessPacket(InputPacket *pkt) {
  LOG_TRACE("message type: %c", pkt->msg_type);
  // We don't set force_flush to true for `PBDE` messages because they're
  // part of the extended protocol. Buffer responses and don't flush until
  // we see a SYNC
  switch (pkt->msg_type) {
    case SIMPLE_QUERY_COMMAND: {
      LOG_TRACE("SIMPLE_QUERY_COMMAND");
      ExecQueryMessage(pkt);
      force_flush = true;
    } break;
    case PARSE_COMMAND: {
      LOG_TRACE("PARSE_COMMAND");
      ExecParseMessage(pkt);
    } break;
    case BIND_COMMAND: {
      LOG_TRACE("BIND_COMMAND");
      ExecBindMessage(pkt);
    } break;
    case DESCRIBE_COMMAND: {
      LOG_TRACE("DESCRIBE_COMMAND");
      return ExecDescribeMessage(pkt);
    } break;
    case EXECUTE_COMMAND: {
      LOG_TRACE("EXECUTE_COMMAND");
      ExecExecuteMessage(pkt);
    } break;
    case SYNC_COMMAND: {
      LOG_TRACE("SYNC_COMMAND");
      SendReadyForQuery(txn_state_);
      force_flush = true;
    } break;
    case TERMINATE_COMMAND: {
      LOG_TRACE("TERMINATE_COMMAND");
      force_flush = true;
      return false;
    } break;
    case NULL: {
      LOG_TRACE("NULL");
      force_flush = true;
      return false;
    } break;
    default: {
      LOG_ERROR("Packet type not supported yet: %d (%c)", pkt->msg_type,
                pkt->msg_type);
    }
  }
  return true;
}

/*
 * send_error_response - Sends the passed string as an error response.
 *    For now, it only supports the human readable 'M' message body
 */
void PacketManager::SendErrorResponse(
    std::vector<std::pair<uchar, std::string>> error_status) {
  std::unique_ptr<OutputPacket> pkt(new OutputPacket());
  pkt->msg_type = ERROR_RESPONSE;

  for (auto entry : error_status) {
    PacketPutByte(pkt.get(), entry.first);
    PacketPutString(pkt.get(), entry.second);
  }

  // put null terminator
  PacketPutByte(pkt.get(), 0);

  // don't care if write finished or not, we are closing anyway
  responses.push_back(std::move(pkt));
}

void PacketManager::SendReadyForQuery(uchar txn_status) {
  std::unique_ptr<OutputPacket> pkt(new OutputPacket());
  pkt->msg_type = READ_FOR_QUERY;

  PacketPutByte(pkt.get(), txn_status);

  responses.push_back(std::move(pkt));
}

void PacketManager::Reset() {
  client_.Reset();
  is_started = false;
  force_flush = false;

  responses.clear();
  unnamed_statement_.reset();
  result_format_.clear();
  txn_state_ = TXN_IDLE;
  skipped_stmt_ = false;
  skipped_query_string_.clear();
  skipped_query_type_.clear();

  statement_cache_.clear();
  portals_.clear();
  pkt_cntr_ = 0;
}

}  // End wire namespace
}  // End peloton namespace
