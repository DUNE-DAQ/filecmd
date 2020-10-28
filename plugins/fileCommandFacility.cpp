/** @file fileCommandFacility.cpp

    N.B. this file starts lower case on purpose.  Do not "fix" it.

    Read commands from various types of file.
*/

// Fixme: The local "streams" types implemented locallyy are general
// and could be useful broken out.
// 
// Fixme: need to support more file schemes, eg with http://,
// zeromq://, gRPC://, etc.

#include "cmdlib/Issues.hpp"
#include "cmdlib/CommandFacility.hpp"

#include <folly/Uri.h>
#include <cetlib/BasicPluginFactory.h>
#include <nlohmann/json.hpp>

#include <fstream>
#include <string>
#include <memory>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h> 

using namespace dunedaq::cmdlib;

using object_t = nlohmann::json;

// Abstract the object level
struct ObjectStream {
    std::string name;
    std::fstream& io;

    ObjectStream(std::string name, std::fstream& io) : name(name), io(io) {
        if (!io) {
            throw BadFile(ERS_HERE, name,"");
        }
    }
    virtual ~ObjectStream(){}

    // One could, but in this case, one should not.
    ObjectStream(const ObjectStream&) = delete;
    ObjectStream(const ObjectStream&&) = delete;
    ObjectStream& operator=(const ObjectStream&) = delete;
    ObjectStream& operator=(const ObjectStream&&) = delete;

    // Get the next object from the stream.
    virtual object_t get() = 0;

    // Put an object to the stream.
    virtual void put(object_t) = 0;

    // For object streams that buffer, may expclictly flush prior to
    // destruction.
    virtual void flush() {}

    // Get stream, checked okay for reading
    std::fstream& r() {
        if (io.eof()) {
            ERS_INFO("EOF: " << name);
            throw StreamExhausted(ERS_HERE, name, "EOF");
        }
        if (! io.good()) {
            throw InternalError(ERS_HERE, "stream bad: " + name);
        }
        return io;
    }

    // Get stream, checked okay for writing
    std::fstream& w() {
        if (! io.good()) {
            throw InternalError(ERS_HERE, "stream bad: " + name);
        }
        return io;
    }

};




// Interpret byte stream as a JSON stream.
// https://en.wikipedia.org/wiki/JSON_streaming.
struct JsonStream : ObjectStream {   
    virtual ~JsonStream() {}
    bool loop_ = false;         // reopen FIFOs on EOF if true
    JsonStream(std::string name, std::fstream& io) : ObjectStream(name, io) {
        struct stat s;
        int rc = stat(name.c_str(), &s);
        if (rc != 0) {
            throw BadFile(ERS_HERE, name, "failed to stat");
        }
        if (S_ISFIFO(s.st_mode)) {
            loop_ = true;
        }
    }
    // One could, but in this case, one should not.
    JsonStream(const JsonStream&) = delete;
    JsonStream(const JsonStream&&) = delete;
    JsonStream& operator=(const JsonStream&) = delete;
    JsonStream& operator=(const JsonStream&&) = delete;


    virtual object_t get() {
        object_t obj;
        try {
            r() >> obj;
        }
        catch (const object_t::parse_error& pe) {
            if (pe.id == 101) {
                if (loop_) {
                    io.close();
                    io.clear();
                    io.open(name, std::ios_base::in);
                    return get();
                }
                ERS_INFO("EOF: " << name);
                throw StreamExhausted(ERS_HERE, name, "EOF");
            }
            throw StreamCorrupt(ERS_HERE, name, pe.what());
        }
        if (! obj.is_object()) {
            std::string msg = "want: object, got: ";
            msg += obj.dump(4); // fixme: temp for debugging!
            throw StreamCorrupt(ERS_HERE, name, msg);
        }
        return obj;
    }
    
    virtual void put(object_t obj) {
        w() << obj;             // what could go wrong?
    }
};

// Interpret a byte stream as a JSON Array.  This slurps input or
// buffers output.  Multiple calls to flush() will effectively produce
// a JSON Stream of arrays of objects.
struct JsonArray : public ObjectStream {
    object_t arr;
    bool isread;
    JsonArray(std::string name, std::fstream& io, bool isread=true)
        : ObjectStream(name, io)
        , isread(isread) {
        arr = object_t::array();
        if (isread) slurp();
    }
    virtual ~JsonArray() { if (!isread) { this->flush(); } }

    // One could, but in this case, one should not.
    JsonArray(const JsonArray&) = delete;
    JsonArray(const JsonArray&&) = delete;
    JsonArray& operator=(const JsonArray&) = delete;
    JsonArray& operator=(const JsonArray&&) = delete;

    void slurp() {
        try {
            r() >> arr;
        }
        catch (const object_t::parse_error& pe) {
            throw StreamCorrupt(ERS_HERE, name, pe.what());
        }
        if (! arr.is_array()) {
            std::string msg = "want: array, got: ";
            msg += arr.dump(4); // fixme: temp for debugging!
            throw StreamCorrupt(ERS_HERE, name, msg);
        }
    }

    virtual object_t get() {
        if (arr.empty()) {
            ERS_INFO("EOF: " << name);
            throw StreamExhausted(ERS_HERE, name, "array end");
        }
        auto obj = arr[0];
        arr.erase(0);
        if (! obj.is_object()) {
            std::string msg = "want: object, got: ";
            msg += obj.dump(4); // fixme: temp for debugging!
            throw StreamCorrupt(ERS_HERE, name, msg);
        }
        return obj;
    }

    virtual void put(object_t obj) {
        arr.push_back(obj);
    }

    virtual void flush() {
        if (arr.empty()) { return; }
        w() << arr;
        arr.empty();
    }
};


// Note: this is registered as fileCommandFacility (lower-case "f").
//
// It actually parses a URL of input.  If a file:// scheme or a simple
// file system path is given, the FCF may handle it.
//
// The URL may be like
//
// file://relative.json
// file:///absolute/path/file.jstream
//
// The leading file:// may be omitted.
//
// An overriding format may be given:
//
// file:///dev/stdin?fmt=json
// not-truly-json.json?fmt=jstream
// 
class fileCommandFacility : public dunedaq::cmdlib::CommandFacility {
public:
    virtual ~fileCommandFacility() {
        // assure these die first.
        ios.reset();
    }


    fileCommandFacility(std::string uri) : CommandFacility(uri) {
        if (uri.size() < 5 or "file:" != uri.substr(0, 5)) {
            if (uri[0] == '/') {
                uri = "file://" + uri;
            }
            else {
                uri = "file:" + uri;
            }
        }

        folly::Uri puri(uri);
        ERS_INFO("uri: " << uri);
        const auto path = puri.path();
        const auto scheme = puri.scheme();
        const auto queryparams = puri.getQueryParams();

        ERS_INFO("url: scheme:" << scheme
                 << " path:" << path);

        if (!(scheme.empty() || scheme == "file")) {
            ERS_INFO("unknown scheme for URL: " << uri);
            throw UnsupportedUri(ERS_HERE, uri);
        }
        if (path.empty()) {
            ERS_INFO("no path found for URL: " << uri);
            throw UnsupportedUri(ERS_HERE, uri);
        }

        auto dot = path.find_last_of(".");
        std::string fmt = path.substr(dot+1);
        for (const auto& kv : queryparams) {
            if (kv.first == "fmt") {
                fmt = kv.second;
            }
        }
        if (fmt.empty()) {
            ERS_INFO("no format found for URL: " << uri);
            throw UnsupportedUri(ERS_HERE, uri);
        }
        if (!(fmt == "json" || fmt == "jstream")) {
            ERS_INFO("unknown format: " << fmt << " from: " << uri);
            throw UnsupportedUri(ERS_HERE, uri);
        }

        ERS_INFO("open: " << uri << " as " << fmt);

        istr.open(path, std::ios_base::in);
        if (fmt == "json") {
            ios.reset(new JsonArray(path, istr, true));
            return;
        }
        if (fmt == "jstream") {
            ios.reset(new JsonStream(path, istr));
            return;
        }
        assert(false);          // can not happen
    }


    object_t recv() const {
        return ios->get();
    }


    void run(std::atomic<bool>& end_marker) {
        while (end_marker) {
            object_t command;

            try {
                command = this->recv();
            }
            catch (const StreamExhausted& e) {
                ERS_INFO("Command stream end");
                break;
            }
            //manager.execute(command);
            inherited::executeCommand(command);
            ERS_INFO("DAQModuleManager execution complete");
        }
        ios.reset();
    }

protected:
  typedef CommandFacility inherited;

  // Implementation of completionHandler interface
  void completionCallback(const std::string& result) {
    ERS_INFO("Command execution resulted with: " << result);
  }

private:
  std::fstream istr;
  std::unique_ptr<ObjectStream> ios;

};

extern "C" {
    std::shared_ptr<dunedaq::cmdlib::CommandFacility> make(std::string uri) { 
        return std::shared_ptr<dunedaq::cmdlib::CommandFacility>(new fileCommandFacility(uri));
    }
}
