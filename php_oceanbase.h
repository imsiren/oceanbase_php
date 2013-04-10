#ifndef PHP_OCEANBASE_H
#define PHP_OCEANBASE_H

#define PHP_OCEANBASE_EXTNAME  "oceanbase"
#define PHP_OCEANBASE_EXTVER   "1.0"

extern zend_module_entry oceanbase_module_entry;
#define phpext_oceanbase_ptr &oceanbase_module_entry

#ifdef PHP_WIN32
#	define PHP_OCEANBASE_API __declspec(dllexport)
#elif defined(__GNUC__) && __GNUC__ >= 4
#	define PHP_OCEANBASE_API __attribute__ ((visibility("default")))
#else
#	define PHP_OCEANBASE_API
#endif

extern "C"{
#ifdef ZTS
#include "TSRM.h"
#endif
}

PHP_MINIT_FUNCTION(oceanbase);
PHP_MSHUTDOWN_FUNCTION(oceanbase);
PHP_MINFO_FUNCTION(oceanbase);

//构造与析构
PHP_METHOD(OceanBase, __construct);
PHP_METHOD(OceanBase, __destruct);

PHP_METHOD(OceanBase, get);
PHP_METHOD(OceanBase, mget);
PHP_METHOD(OceanBase, scan);
PHP_METHOD(OceanBase, insert);
PHP_METHOD(OceanBase, minsert);
PHP_METHOD(OceanBase, update);
PHP_METHOD(OceanBase, mupdate);
PHP_METHOD(OceanBase, delete);
PHP_METHOD(OceanBase, mdelete);
PHP_METHOD(OceanBase, fetchRowNum);

ZEND_BEGIN_MODULE_GLOBALS(oceanbase)
	char* log_level;
	char *log_file;
ZEND_END_MODULE_GLOBALS(oceanbase)

PHPAPI void print_error_info(const char* error_str TSRMLS_DC);
#ifdef ZTS
#define OCEANBASE_G(v) TSRMG(oceanbase_globals_id, zend_oceanbase_globals *, v)
#else
#define OCEANBASE_G(v) (oceanbase_globals.v)
#endif

#endif	/* PHP_OCEANBASE_H */


/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
