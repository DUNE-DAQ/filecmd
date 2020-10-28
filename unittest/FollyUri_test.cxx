#include <string>
#include <iostream>
#include <folly/Uri.h>


static
void run_dump(std::string uri)
{
    folly::Uri puri(uri);
    std::cout << "[" << uri << "]" << std::endl;
    std::cout << "Scheme: " << puri.scheme() << std::endl;
    std::cout << "Host:   " << puri.host() << std::endl;
    std::cout << "Port:   " << puri.port() << std::endl;
    std::cout << "Path:   " << puri.path() << std::endl;
    std::cout << "User:   " << puri.username() << std::endl;
    std::cout << "Pass:   " << puri.password() << std::endl;
    std::cout << "Frag:   " << puri.fragment() << std::endl;

    const auto& queries = puri.getQueryParams();
    std::cout << "Queries: [" << queries.size() << "]" << std::endl;
    for (const auto& [key,val] : queries) {
        std::cout << "\t\"" << key << "\" = \"" << val << "\"" << std::endl;
    }
    std::cout << "-------------------------------" << std::endl;

}

int main(int argc, char* argv[])
{
    try {
        run_dump("simplefile.json");
    }
    catch (const std::invalid_argument& err) {
        std::cerr << "Folly::Uri does not support simple file paths"
                  << std::endl;

    }
    run_dump("file:zero.json");
    run_dump("file://two.json?illegal=yes&fileis=domain");
    run_dump("file:///three.json");
    run_dump("file:////four.json?microsoft=sucks&dont=doit");
    run_dump("file:relative-with-param.json?fmt=jstream&another=42");
    try {
        run_dump("relative-with-param.json?fmt=jstream&another=42");
    }
    catch (const std::invalid_argument& err) {
        std::cerr
            << "Folly::Uri does not support relative file path with params"
            << std::endl;
    }
    try {
        run_dump("/dev/stdin?fmt=jstream&another=42");
    }
    catch (const std::invalid_argument& err) {
        std::cerr
            << "Folly::Uri does not support absolute file path with params"
            << std::endl;
    }
    run_dump("http://example.com:4321/path?foo=bar");
    run_dump("http://user:pass@example.com:4321/path?foo=bar");
    run_dump("mailto:someone@example.com");
    for (int iarg=1; iarg < argc; ++iarg) {
        run_dump(argv[iarg]);
        //const std::string maybe = argv[iarg];
    }
    return 0;
}
