#include "MyException.h"

namespace evcrawler
{
    MyException::MyException(const char * message, const char* filename, uint32_t line, const char * method)
    {
        snprintf(m_message,  sizeof(m_message),  "%s", message);
        snprintf(m_filename, sizeof(m_filename), "%s", filename);
        snprintf(m_method, sizeof(m_method), "%s", method);
        m_line = line;
    }
}
