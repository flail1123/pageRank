#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"
#include <string>

class Sha256IdGenerator : public IdGenerator {
public:
    virtual PageId generateId(std::string const& content) const
    {
        std::string pom = "printf \"" + content + "\" | sha256sum ";
        const char* line = pom.c_str();

        FILE* file = popen(line, "r");
        char* result = new char[64];
        if (fscanf(file, "%64s", result) == 1) {
            pclose(file);
            return PageId(result);
        }
        pclose(file);

        ASSERT(false, "Popen failed");
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
