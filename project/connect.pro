
TEMPLATE = app
CONFIG += console c++14 object_parallel_to_source
CONFIG -= app_bundle
#CONFIG -= qt

LIBS += -lpthread -lpq
LIBS += -L../lib/botan/ -lbotan-2

INCLUDEPATH += ../source
INCLUDEPATH += ../lib/utils
INCLUDEPATH += ../lib/libpq
INCLUDEPATH += ../lib/libpqxx/include
INCLUDEPATH += ../lib/fmt/include
INCLUDEPATH += ../lib/json
INCLUDEPATH += ../lib/botan/build/include
INCLUDEPATH += ../lib/picohttpparser
INCLUDEPATH += ../lib

TARGET = connect
INSTALLS        = target
target.files    = connect
target.path     = /alles/bin

SOURCES += \
    ../source/entry_point.cpp \
    ../lib/opcua/client.cpp \
    ../lib/opcua/variant.cpp \
    ../lib/opcua/open62541.c \
    ../lib/utils/utils.cpp \
    ../lib/libpqxx/src/array.cxx \
    ../lib/libpqxx/src/binarystring.cxx \
    ../lib/libpqxx/src/connection_base.cxx \
    ../lib/libpqxx/src/connection.cxx \
    ../lib/libpqxx/src/cursor.cxx \
    ../lib/libpqxx/src/dbtransaction.cxx \
    ../lib/libpqxx/src/errorhandler.cxx \
    ../lib/libpqxx/src/except.cxx \
    ../lib/libpqxx/src/field.cxx \
    ../lib/libpqxx/src/largeobject.cxx \
    ../lib/libpqxx/src/nontransaction.cxx \
    ../lib/libpqxx/src/notification.cxx \
    ../lib/libpqxx/src/pipeline.cxx \
    ../lib/libpqxx/src/prepared_statement.cxx \
    ../lib/libpqxx/src/result.cxx \
    ../lib/libpqxx/src/robusttransaction.cxx \
    ../lib/libpqxx/src/row.cxx \
    ../lib/libpqxx/src/statement_parameters.cxx \
    ../lib/libpqxx/src/strconv.cxx \
    ../lib/libpqxx/src/subtransaction.cxx \
    ../lib/libpqxx/src/tablereader.cxx \
    ../lib/libpqxx/src/tablestream.cxx \
    ../lib/libpqxx/src/tablewriter.cxx \
    ../lib/libpqxx/src/transaction_base.cxx \
    ../lib/libpqxx/src/transaction.cxx \
    ../lib/libpqxx/src/util.cxx \
    ../lib/libpqxx/src/version.cxx \
    ../lib/fmt/src/format.cc \
    ../lib/fmt/src/posix.cc \
    ../lib/libcm/data.cpp \
    ../lib/libcm/ip.cpp \
    ../lib/libcm/socket.posix.cpp \
    ../lib/libcm/protocol/kitcash.cpp \
    ../source/session_rpc.cpp \
    ../lib/libcm/protocol/http.cpp \
    ../lib/picohttpparser/picohttpparser.c \
    ../source/services/carwash.cpp \
    ../source/services/rpc.cpp \
    ../source/services/settings.cpp \
    ../source/services/storage.cpp \
    ../lib/utils/logger.cpp \
    ../lib/utils/event_loop.cpp \
    ../lib/utils/hex.cpp \
    ../lib/libcm/protocol/tls.cpp \
    ../source/services/kkt.cpp \
    ../source/services/bonus.cpp \
    ../lib/utils/time.cpp





