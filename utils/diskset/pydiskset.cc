#include <Python.h>
#include "diskset.h"

extern "C"
{
    static void py_del_diskset(void *ptr)
    {
        diskset* pdiskset = static_cast<diskset *>(ptr);
        delete pdiskset;
        return;
    }

    PyObject* wrap_new_diskset(PyObject* self, PyObject* args) 
    {
        const char* data_dir = NULL;
        uint32_t clear_flag = 0;
        if (! PyArg_ParseTuple(args, "sI", &data_dir, &clear_flag))
        {
            return NULL;
        }
        diskset* pdiskset = new diskset(data_dir, clear_flag != 0);
        return PyCObject_FromVoidPtr(pdiskset, py_del_diskset);
    }

    PyObject* wrap_select_key(PyObject* self, PyObject* args) 
    {
        uint64_t key;
        uint32_t* sign = (uint32_t*)&key;
        PyObject *pydiskset = NULL;
        if (! PyArg_ParseTuple(args, "OII", &pydiskset, &sign[0], &sign[1]))
        {
            return NULL;
        }

        //把PyCObject转换为void指针
        diskset * this_diskset = static_cast<diskset *>(PyCObject_AsVoidPtr(pydiskset));
        //调用函数
        return (PyObject*)Py_BuildValue("i", this_diskset->select_key(key));
    }

    PyObject* wrap_insert_key(PyObject* self, PyObject* args) 
    {
        uint64_t key;
        uint32_t* sign = (uint32_t*)&key;
        PyObject *pydiskset = NULL;
        if (! PyArg_ParseTuple(args, "OII", &pydiskset, &sign[0], &sign[1]))
        {
            return NULL;
        }

        //把PyCObject转换为void指针
        diskset * this_diskset = static_cast<diskset *>(PyCObject_AsVoidPtr(pydiskset));
        //调用函数
        return (PyObject*)Py_BuildValue("i", this_diskset->insert_key(key));
    }

    PyObject* wrap_clear(PyObject* self, PyObject* args) 
    {
        PyObject *pydiskset = 0;
        int ok = PyArg_ParseTuple( args, "O", &pydiskset);
        if(!ok) return NULL;
        //把PyCObject转换为void指针

        diskset * this_diskset = static_cast<diskset *>(PyCObject_AsVoidPtr(pydiskset));
        this_diskset->clear();
        //调用函数
        return (PyObject*)Py_BuildValue("i", 0);
    }

    static PyMethodDef disksetMethods[] = 
    {
        {"pydiskset",  wrap_new_diskset, METH_VARARGS, "diskset for unique"},
        {"select_key", wrap_select_key,  METH_VARARGS, "check the key in set or not"},
        {"insert_key", wrap_insert_key,  METH_VARARGS, "insert the key into set"},
        {"clear",      wrap_clear,       METH_VARARGS, "clear the data"},
        {NULL, NULL}
    };

    void initpydiskset() 
    {
        PyObject* m;
        m = Py_InitModule("pydiskset", disksetMethods);
    }
}
