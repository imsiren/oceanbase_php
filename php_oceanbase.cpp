extern "C"{

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
}

#include "php_oceanbase.h"
#include "oceanbase.h"

#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <pthread.h>
using namespace std;

ZEND_DECLARE_MODULE_GLOBALS(oceanbase);
/* True global resources - no need for thread safety here */
static map<string, OB*>* global_ob_pool = NULL;
static pthread_rwlock_t rwlock;

/*declare the class handles*/
zend_object_handlers oceanbase_object_handlers;

/*declare the class entry*/
zend_class_entry *oceanbase_ce;

/* the overloaded class structure */
typedef struct _oceanbase_object {
	zend_object std;
	OB *ob;
} oceanbase_object;

/* close all resources and the memory allocated for the object */
void oceanbase_object_free_storage(void *object TSRMLS_DC)
{
	oceanbase_object *intern = (oceanbase_object *)object;


	zend_hash_destroy(intern->std.properties);
	FREE_HASHTABLE(intern->std.properties);

	efree(intern);
}

/* creates the object by 
   - allocating memory 
   - initializing the object members
   - storing the object
   - setting it's handlers

   called from 
   - clone
   - new

TODO: modify for clone
 */
static zend_object_value oceanbase_object_create(zend_class_entry *type TSRMLS_DC)
{
	zval *tmp;
	zend_object_value retval;

	oceanbase_object *obj = (oceanbase_object *)emalloc(sizeof(oceanbase_object));
	memset(obj, 0, sizeof(oceanbase_object));
	obj->std.ce = type;

	ALLOC_HASHTABLE(obj->std.properties);
	zend_hash_init(obj->std.properties, 0, NULL, ZVAL_PTR_DTOR, 0);
	zend_hash_copy(obj->std.properties, &type->default_properties,
			(copy_ctor_func_t)zval_add_ref, (void *)&tmp, sizeof(zval *));

	retval.handle = zend_objects_store_put(obj, NULL,
			oceanbase_object_free_storage, NULL TSRMLS_CC);
	retval.handlers = &oceanbase_object_handlers;

	return retval;
}

/*the method table*/
	const zend_function_entry oceanbase_functions[] = {
		PHP_ME(OceanBase, __construct, NULL, ZEND_ACC_PUBLIC|ZEND_ACC_CTOR)
			PHP_ME(OceanBase, __destruct, NULL, ZEND_ACC_PUBLIC|ZEND_ACC_DTOR)

			PHP_ME(OceanBase, get, NULL, ZEND_ACC_PUBLIC)
			PHP_ME(OceanBase, mget, NULL, ZEND_ACC_PUBLIC)
			PHP_ME(OceanBase, scan, NULL, ZEND_ACC_PUBLIC)

			PHP_ME(OceanBase, insert, NULL, ZEND_ACC_PUBLIC)
			PHP_ME(OceanBase, minsert, NULL, ZEND_ACC_PUBLIC)
			PHP_ME(OceanBase, update, NULL, ZEND_ACC_PUBLIC)
			PHP_ME(OceanBase, mupdate, NULL, ZEND_ACC_PUBLIC)
			PHP_ME(OceanBase, delete, NULL, ZEND_ACC_PUBLIC)
			PHP_ME(OceanBase, mdelete, NULL, ZEND_ACC_PUBLIC)
			PHP_ME(OceanBase, fetchRowNum, NULL, ZEND_ACC_PUBLIC)

			{NULL, NULL, NULL}	/* Must be the last line in oceanbase_functions[] */
	};

/*oceanbase_module_entry*/
zend_module_entry oceanbase_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
	STANDARD_MODULE_HEADER,
#endif
	PHP_OCEANBASE_EXTNAME, /*name*/
	NULL, /* Functions */
	PHP_MINIT(oceanbase), /* MINIT */
	PHP_MSHUTDOWN(oceanbase), /* MSHUTDOWN */
	NULL, /* RINIT */
	NULL, /* RSHUTDOWN */
	PHP_MINFO(oceanbase), /* MINFO */
#if ZEND_MODULE_API_NO >= 20010901
	PHP_OCEANBASE_EXTVER, /* Replace with version number for your extension */
#endif
	STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_OCEANBASE
ZEND_GET_MODULE(oceanbase)
#endif

PHP_INI_BEGIN()
    STD_PHP_INI_ENTRY("oceanbase.log_file",NULL,PHP_INI_ALL,OnUpdateString,log_file,zend_oceanbase_globals,oceanbase_globals)
	STD_PHP_INI_ENTRY("oceanbase.log_level",NULL,PHP_INI_ALL,OnUpdateString,log_level,zend_oceanbase_globals,oceanbase_globals)
PHP_INI_END()

PHP_MINIT_FUNCTION(oceanbase) {
	/* If you have INI entries, uncomment these lines */
	REGISTER_INI_ENTRIES();
	zend_class_entry ce;

	INIT_CLASS_ENTRY(ce, "OceanBase", oceanbase_functions);
	oceanbase_ce = zend_register_internal_class(&ce TSRMLS_CC);
	oceanbase_ce->create_object = oceanbase_object_create;
	memcpy(&oceanbase_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
	oceanbase_object_handlers.clone_obj = NULL;

	global_ob_pool = new map<string, OB*>();
	int res = pthread_rwlock_init(&rwlock, NULL);
	if(res != 0)
	{
		delete global_ob_pool;
		return false;
	}
	
	return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(oceanbase)
{
	/* uncomment this line if you have INI entries*/
	UNREGISTER_INI_ENTRIES();
	for(map<string, OB*>::iterator it = global_ob_pool->begin(); it!=global_ob_pool->end(); it++)
	{
		ob_api_destroy(it->second);
	}
	global_ob_pool->clear();
	delete global_ob_pool;

	pthread_rwlock_destroy(&rwlock);
	return SUCCESS;
}



PHP_MINFO_FUNCTION(oceanbase)
{
	php_info_print_table_start();
	php_info_print_table_header(2, "oceanbase support", "enabled");
	php_info_print_table_end();

	/* Remove comments if you have entries in php.ini*/
	DISPLAY_INI_ENTRIES();

}

/*error log*/
PHPAPI void print_error_info(const char * error_str TSRMLS_DC)
{
	//php_printf("%s\n", error_str);	
    cerr<<error_str<<endl;
}

/* construct
   @param $server_ip
   @param $server_port
   @param $user
   @param $passwd
 */
PHP_METHOD(OceanBase, __construct) 
{
	//php_printf("__construct called.\n");

	char *arg_server_ip = NULL;
	int arg_server_ip_len;

	int64_t arg_server_port;

	char *arg_user = NULL;
	int arg_user_len;

	char *arg_passwd = NULL;
	int arg_passwd_len;
   
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);
	obj->ob = NULL;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sl|ss", &arg_server_ip, &arg_server_ip_len, &arg_server_port,
				&arg_user, &arg_user_len, &arg_passwd, &arg_passwd_len) == FAILURE) {
		RETURN_FALSE;
	}

	string server_port_str;
	stringstream port_stream;
	port_stream << arg_server_port;
	port_stream >> server_port_str;
	string server_ip_str = string(arg_server_ip, arg_server_ip_len);
	string server_str = server_ip_str + string(":") + server_port_str;

	pthread_rwlock_rdlock(&rwlock);
	map<string, OB*>::iterator server_it = global_ob_pool->find(server_str);
	if(server_it != global_ob_pool->end())
	{
		obj->ob = server_it->second;
		pthread_rwlock_unlock(&rwlock); 
	}
	else
	{
		pthread_rwlock_unlock(&rwlock);
		obj->ob = ob_api_init();
		if (NULL == obj->ob)
		{
			string err_str = "ob_init error";
			print_error_info(err_str.c_str() TSRMLS_CC);
			RETURN_FALSE;
		}

		ob_api_cntl(obj->ob, OB_S_TIMEOUT, 100000000);

		OB_ERR_CODE err = ob_connect(obj->ob, arg_server_ip, arg_server_port, arg_user, arg_passwd);
		if (OB_ERR_SUCCESS != err)
		{
			ob_api_destroy(obj->ob);
			obj->ob = NULL;

			string err_str = "ob_connect error";
			print_error_info(err_str.c_str() TSRMLS_CC);
			RETURN_FALSE;
		}
		pthread_rwlock_wrlock(&rwlock);
		global_ob_pool->insert(make_pair(server_str, obj->ob));
		pthread_rwlock_unlock(&rwlock);
	}
   	if(strlen(OCEANBASE_G(log_level))>0){
		ob_api_debug_log(obj->ob,OCEANBASE_G(log_level),OCEANBASE_G(log_file));
	} 

	RETURN_TRUE;

}

PHP_METHOD(OceanBase, __destruct) 
{
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);

	if(obj->ob != NULL)
	{
		//TODO OB_ERR_CODE err = ob_close(obj->ob);
		//ob_api_destroy(obj->ob);
		obj->ob = NULL;
	}
	//php_printf("__destruct called.\n");
}

/* single table and single rowkey 
   @param string $table
   @param string $rowkey
   @param array $columns
   @return array $results
 */
PHP_METHOD(OceanBase, get)
{
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);

	char* arg_table = NULL;
	int arg_table_len;

	char* arg_row_key = NULL;
	int arg_row_key_len;

	zval *columns_arr,**columns_data;
	HashTable *columns_hash;
	HashPosition columns_pointer;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ssa", 
				&arg_table, &arg_table_len, 
				&arg_row_key, &arg_row_key_len, 
				&columns_arr) == FAILURE) {
		RETURN_FALSE;
	}

	OB_GET* ob_get_st = ob_acquire_get_st(obj->ob);
	if (ob_get_st == NULL)
	{
		string err_str = "ob_acquire_get_st error";
		print_error_info(err_str.c_str() TSRMLS_CC);

		RETURN_FALSE;
	}
	columns_hash = Z_ARRVAL_P(columns_arr);

	for(zend_hash_internal_pointer_reset_ex(columns_hash, &columns_pointer);
			zend_hash_get_current_data_ex(columns_hash, (void**)&columns_data, &columns_pointer)==SUCCESS;
			zend_hash_move_forward_ex(columns_hash, &columns_pointer))
	{
		if(Z_TYPE_PP(columns_data) != IS_STRING)
			continue;

		zval arg_column = **columns_data;
		zval_copy_ctor(&arg_column);
		convert_to_string(&arg_column);

		ob_get_cell(ob_get_st, arg_table, arg_row_key, arg_row_key_len, Z_STRVAL(arg_column));

		zval_dtor(&arg_column);
	}

	if(OB_ERR_SUCCESS != ob_errno())
	{
		string err_str = "ob_get_cell error";
		print_error_info(err_str.c_str() TSRMLS_CC);

		ob_release_get_st(obj->ob, ob_get_st);
		ob_get_st = NULL;
        ob_set_errno(0);
		RETURN_FALSE;
	}

	OB_RES* ob_res_st = ob_exec_get(obj->ob, ob_get_st);
	if (ob_res_st == NULL)
	{
		string err_str = "ob_exec_get error";
		print_error_info(err_str.c_str() TSRMLS_CC);

		ob_release_get_st(obj->ob, ob_get_st);
		ob_get_st = NULL;
		RETURN_FALSE;
	}

	OB_CELL* cell = NULL;

	array_init(return_value);
	bool is_null = false;
	while ((cell = ob_fetch_cell(ob_res_st)) != NULL)
	{
		if (cell->is_row_not_exist)
			is_null = true;

		string temp_str = string(cell->column, (int)cell->column_len);
		if(cell->is_null)
			add_assoc_null(return_value, temp_str.c_str());
		else if(cell->v.type == OB_INT_TYPE)
			add_assoc_long(return_value, temp_str.c_str(), cell->v.v.v_int);
		else if(cell->v.type == OB_VARCHAR_TYPE)
			add_assoc_stringl(return_value, temp_str.c_str(), cell->v.v.v_varchar.p, (int)cell->v.v.v_varchar.len, 1);
		else if(cell->v.type == OB_DATETIME_TYPE)
			add_assoc_long(return_value, temp_str.c_str(), 1000 * cell->v.v.v_datetime.tv_sec + cell->v.v.v_datetime.tv_usec/1000);
	}
	//TODO: loop for all results    
	ob_release_res_st(obj->ob, ob_res_st);
	ob_res_st = NULL;
	ob_release_get_st(obj->ob, ob_get_st);
	ob_get_st = NULL;

	if(is_null)
		RETURN_NULL();
}

PHP_METHOD(OceanBase, mget)
{
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);

	zval *table_arr,**table_data;
	char *table_name;
	uint table_name_len;
	zval *key_arr;
	HashTable *table_hash;
	HashPosition table_pointer;

	if(zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "a", &table_arr) == SUCCESS ) {
		;
	}
	else if(zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "sa", 
				&table_name, &table_name_len, &key_arr) == SUCCESS ) {
		ALLOC_INIT_ZVAL(table_arr);
		array_init(table_arr);
		add_assoc_zval(table_arr, table_name, key_arr);
	}
	else {
		php_error(E_WARNING, "arguments error\n");
		RETURN_FALSE;
	}


	OB_GET* ob_get_st = ob_acquire_get_st(obj->ob);
	if (ob_get_st == NULL)
	{
		string err_str = "ob_acquire_get_st error";
		print_error_info(err_str.c_str() TSRMLS_CC);

		RETURN_FALSE;
	}

	table_hash = Z_ARRVAL_P(table_arr);

	for(zend_hash_internal_pointer_reset_ex(table_hash, &table_pointer);
			zend_hash_get_current_data_ex(table_hash, (void**)&table_data, &table_pointer)==SUCCESS;
			zend_hash_move_forward_ex(table_hash, &table_pointer))
	{
		char *arg_table;
		uint arg_table_len;
		uint64_t arg_table_index;

		if(zend_hash_get_current_key_ex(table_hash, &arg_table, &arg_table_len,
					&arg_table_index, 0, &table_pointer) != HASH_KEY_IS_STRING)
			continue;

		if(Z_TYPE_PP(table_data) != IS_ARRAY)
			continue;

		zval *row_key_arr,**row_key_data;
		HashTable *row_key_hash;
		HashPosition row_key_pointer;

		row_key_arr = *table_data;
		row_key_hash = Z_ARRVAL_P(row_key_arr);

		for(zend_hash_internal_pointer_reset_ex(row_key_hash, &row_key_pointer);
				zend_hash_get_current_data_ex(row_key_hash, (void**)&row_key_data, &row_key_pointer)==SUCCESS;
				zend_hash_move_forward_ex(row_key_hash, &row_key_pointer))
		{
			char *arg_row_key;
			uint arg_row_key_len;
			uint64_t arg_row_key_index;

			if(zend_hash_get_current_key_ex(row_key_hash, &arg_row_key, &arg_row_key_len, 
						&arg_row_key_index, 0, &row_key_pointer) != HASH_KEY_IS_STRING)
				continue;

			if(Z_TYPE_PP(row_key_data) != IS_ARRAY)
				continue;

			zval *column_arr, **column_data;
			HashTable *column_hash;
			HashPosition column_pointer;

			column_arr = *row_key_data;
			column_hash = Z_ARRVAL_P(column_arr);

			for(zend_hash_internal_pointer_reset_ex(column_hash, &column_pointer);
					zend_hash_get_current_data_ex(column_hash, (void**)&column_data, &column_pointer)==SUCCESS;
					zend_hash_move_forward_ex(column_hash, &column_pointer))
			{
				if(Z_TYPE_PP(column_data) != IS_STRING)
					continue;

				zval arg_column = **column_data;
				zval_copy_ctor(&arg_column);
				convert_to_string(&arg_column);

				ob_get_cell(ob_get_st, arg_table, arg_row_key,
						arg_row_key_len - 1, Z_STRVAL(arg_column));

				zval_dtor(&arg_column);
			}
		}
	}

	if(OB_ERR_SUCCESS != ob_errno())
	{
		string err_str = "ob_get_cell error";
		print_error_info(err_str.c_str() TSRMLS_CC);

		ob_release_get_st(obj->ob, ob_get_st);
		ob_get_st = NULL;
        ob_set_errno(0);
		RETURN_FALSE;
	}

	OB_RES* ob_res_st = ob_exec_get(obj->ob, ob_get_st);
	if (ob_res_st == NULL)
	{
		string err_str = "ob_exec_get error";
		print_error_info(err_str.c_str() TSRMLS_CC);

		ob_release_get_st(obj->ob, ob_get_st);
		ob_get_st = NULL;
		RETURN_FALSE;
	}

	OB_CELL* cell = NULL;

	array_init(return_value);
	zval *table_subarray = NULL, *row_key_subarray = NULL;
	string last_table_str,last_row_key_str;
	bool start = true;
	while ((cell = ob_fetch_cell(ob_res_st)) != NULL)
	{
		if(cell->is_row_not_exist)
			continue;
		if(cell->is_row_changed)
		{
			if(!start)
			{
				add_assoc_zval_ex(table_subarray, last_row_key_str.c_str(), last_row_key_str.length(), row_key_subarray);
				zval* temp_row_key;
				row_key_subarray = temp_row_key;
				ALLOC_INIT_ZVAL(row_key_subarray);
				array_init(row_key_subarray);
				last_row_key_str = string(cell->row_key, (int)cell->row_key_len + 1);


				string temp_table_str = string(cell->table, (int)cell->table_len);
				if(last_table_str != temp_table_str )
				{
					add_assoc_zval(return_value, last_table_str.c_str(), table_subarray);
					zval* temp_table;
					table_subarray = temp_table;
					ALLOC_INIT_ZVAL(table_subarray);
					array_init(table_subarray);
					last_table_str = temp_table_str;
				}
			}
			else
			{
				start = false;

				zval* temp_row_key;
				row_key_subarray = temp_row_key;
				ALLOC_INIT_ZVAL(row_key_subarray);
				array_init(row_key_subarray);
				last_row_key_str = string(cell->row_key, (int)cell->row_key_len + 1);

				zval* temp_table;
				table_subarray = temp_table;
				ALLOC_INIT_ZVAL(table_subarray);
				array_init(table_subarray);
				last_table_str = string(cell->table, (int)cell->table_len);
			}
		}

		string column_str = string (cell->column, (int)cell->column_len);

		if(cell->is_null)
			add_assoc_null(row_key_subarray, column_str.c_str());
		else if(cell->v.type == OB_INT_TYPE)
			add_assoc_long(row_key_subarray, column_str.c_str(), cell->v.v.v_int);
		else if(cell->v.type == OB_VARCHAR_TYPE)
			add_assoc_stringl(row_key_subarray, column_str.c_str(), cell->v.v.v_varchar.p, (int)cell->v.v.v_varchar.len, 1);
		else if(cell->v.type == OB_DATETIME_TYPE)
			add_assoc_long(row_key_subarray, column_str.c_str(), 1000 * cell->v.v.v_datetime.tv_sec + cell->v.v.v_datetime.tv_usec/1000);
	}
	if(table_subarray != NULL)
	{
		add_assoc_zval_ex(table_subarray, last_row_key_str.c_str(), last_row_key_str.length(), row_key_subarray);
		add_assoc_zval(return_value, last_table_str.c_str(), table_subarray);
	}

	//TODO: loop for all results    
	ob_release_res_st(obj->ob, ob_res_st);
	ob_res_st = NULL;
	ob_release_get_st(obj->ob, ob_get_st);
	ob_get_st = NULL;

	if(table_subarray == NULL)
		RETURN_NULL();
}

PHP_METHOD(OceanBase, scan)
{
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);

	char* arg_table = NULL;
	int arg_table_len = 0;

	zval *options_arr = NULL,**options_data;
	HashTable *options_hash;
	HashPosition options_pointer;

	zval *arg_range_arr;
	zval *arg_column_arr;
	zval *arg_simple_where_arr;

	if(zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "s|a", 
				&arg_table, &arg_table_len,
				&options_arr) == SUCCESS ) {
		;
	}
	else if(zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "saa",
				&arg_table, &arg_table_len, 
				&arg_range_arr, &arg_column_arr) == SUCCESS ) {
		ALLOC_INIT_ZVAL(options_arr);
		array_init(options_arr);
		add_assoc_zval(options_arr, "range", arg_range_arr);
		add_assoc_zval(options_arr, "columns", arg_column_arr);
	}
	else if(zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "saaa",
                &arg_table, &arg_table_len,
                &arg_range_arr, &arg_column_arr, &arg_simple_where_arr) == SUCCESS ) {
        ALLOC_INIT_ZVAL(options_arr);
        array_init(options_arr);
        add_assoc_zval(options_arr, "range", arg_range_arr);
        add_assoc_zval(options_arr, "columns", arg_column_arr);
		add_assoc_zval(options_arr, "simple_where", arg_simple_where_arr);
    }
	else {
		php_error(E_WARNING, "arguments error\n");
		RETURN_FALSE;
	}

	OB_SCAN* ob_scan_st = ob_acquire_scan_st(obj->ob);
	if(ob_scan_st == NULL)
	{
		string err_str = "ob_acquire_scan_st error";
		print_error_info(err_str.c_str() TSRMLS_CC);
		RETURN_FALSE;
	}

	string row_start = "", row_end = "";
	int64_t start_included = 1, end_included = 1;

	vector<string> columns;

	vector<string> wheres;
	vector<string> simple_wheres;

	vector<string> order_by_columns;
	vector<OB_ORDER> order_by_types;

	vector<string> group_by_columns;

	vector<string> aggregate_columns;
	vector<OB_AGGREGATION_TYPE> aggregate_types;
	vector<string> as_names;

	vector<string> group_by_complex_columns;
	vector<string> group_by_complex_as_names;

	vector<string> havings;

	int64_t limit_offset = 0, limit_count = 0;

	if(options_arr != NULL)
	{
		options_hash = Z_ARRVAL_P(options_arr);
		for(zend_hash_internal_pointer_reset_ex(options_hash, &options_pointer);
				zend_hash_get_current_data_ex(options_hash, (void**)&options_data, &options_pointer)==SUCCESS;
				zend_hash_move_forward_ex(options_hash, &options_pointer))
		{
			char *arg_option;
			uint arg_option_len;
			uint64_t arg_option_index;

			if(zend_hash_get_current_key_ex(options_hash, &arg_option, &arg_option_len,
						&arg_option_index, 0, &options_pointer) != HASH_KEY_IS_STRING)
				continue;

			if(Z_TYPE_PP(options_data) != IS_ARRAY)
				continue;
			//TODO: precision is not array

			zval *option_arr,**option_data;
			HashTable *option_hash;
			HashPosition option_pointer;

			option_arr = *options_data;
			option_hash = Z_ARRVAL_P(option_arr);

			if(strcmp("range", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{
					char *range_type;
					uint range_type_len;
					uint64_t range_type_index;

					if(zend_hash_get_current_key_ex(option_hash, &range_type, &range_type_len,
								&range_type_index, 0, &option_pointer) != HASH_KEY_IS_STRING)
						continue;

					if(Z_TYPE_PP(option_data) != IS_ARRAY)
						continue;

					zval *range_arr,**range_data;
					HashTable *range_hash;
					HashPosition range_pointer;

					range_arr = *option_data;
					range_hash = Z_ARRVAL_P(range_arr);

					for(zend_hash_internal_pointer_reset_ex(range_hash, &range_pointer);
							zend_hash_get_current_data_ex(range_hash, (void**)&range_data, &range_pointer)==SUCCESS;
							zend_hash_move_forward_ex(range_hash, &range_pointer))
					{

						char *range_key;
						uint range_key_len;
						uint64_t range_key_index;

						if(zend_hash_get_current_key_ex(range_hash, &range_key, &range_key_len,
									&range_key_index, 0, &range_pointer) != HASH_KEY_IS_STRING)
							continue;

						zval temp_val;

						if(strcmp("row_key", range_key) == 0)
						{
							if(Z_TYPE_PP(range_data) != IS_STRING)
								continue;

							if(strcmp("start", range_type) ==0)
							{
								temp_val = **range_data;
								zval_copy_ctor(&temp_val);
								convert_to_string(&temp_val);

								row_start = string(Z_STRVAL(temp_val),Z_STRLEN(temp_val));

								zval_dtor(&temp_val);
							}
							else if(strcmp("end", range_type) ==0)
							{
								temp_val = **range_data;
								zval_copy_ctor(&temp_val);
								convert_to_string(&temp_val);

								row_end = string(Z_STRVAL(temp_val),Z_STRLEN(temp_val));

								zval_dtor(&temp_val);
							}
						}
						else if(strcmp("included", range_key) == 0)
						{
							if(Z_TYPE_PP(range_data) != IS_LONG)
								continue;

							if(strcmp("start", range_type) ==0)
							{
								temp_val = **range_data;
								zval_copy_ctor(&temp_val);
								convert_to_long(&temp_val);

								start_included = Z_LVAL(temp_val);

								zval_dtor(&temp_val);
							}
							else if(strcmp("end", range_type) ==0)
							{
								temp_val = **range_data;
								zval_copy_ctor(&temp_val);
								convert_to_long(&temp_val);

								end_included = Z_LVAL(temp_val);

								zval_dtor(&temp_val);
							}
						}

					}
				}
			}
			else if(strcmp("columns", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{   
					if(Z_TYPE_PP(option_data) != IS_STRING)
						continue;

					zval temp_column = **option_data;
					zval_copy_ctor(&temp_column);
					convert_to_string(&temp_column);

					columns.push_back(string(Z_STRVAL(temp_column)));

					zval_dtor(&temp_column);
				}
			}
			else if(strcmp("order_by", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{
					char *column_name;
					uint column_name_len;
					uint64_t column_name_index;

					if(zend_hash_get_current_key_ex(option_hash, &column_name, &column_name_len,
								&column_name_index, 0, &option_pointer) != HASH_KEY_IS_STRING)
						continue;

					if(Z_TYPE_PP(option_data) != IS_STRING)
						continue;

					zval temp_order_by_type = **option_data;
					zval_copy_ctor(&temp_order_by_type);
					convert_to_string(&temp_order_by_type);

					string temp_order_by_type_str = string(Z_STRVAL(temp_order_by_type));
					if(temp_order_by_type_str == "DES")
						order_by_types.push_back(OB_DESC);
					else if(temp_order_by_type_str == "ASC")
						order_by_types.push_back(OB_ASC);
					else
					{
						zval_dtor(&temp_order_by_type);
						continue;	
					}

					order_by_columns.push_back(string(column_name));
					zval_dtor(&temp_order_by_type);
				}
			}
			else if(strcmp("limit", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{
					char *limit_type;
					uint limit_type_len;
					uint64_t limit_type_index;

					if(zend_hash_get_current_key_ex(option_hash, &limit_type, &limit_type_len,
								&limit_type_index, 0, &option_pointer) != HASH_KEY_IS_STRING)
						continue;

					if(Z_TYPE_PP(option_data) != IS_LONG)
						continue;

					zval temp_limit = **option_data;
					zval_copy_ctor(&temp_limit);
					convert_to_long(&temp_limit);

					if(strcmp("offset", limit_type) == 0)
						limit_offset = Z_LVAL(temp_limit);
					else if(strcmp("count", limit_type) == 0)
						limit_count = Z_LVAL(temp_limit);

					zval_dtor(&temp_limit);				
				}
			}
			else if(strcmp("group_by", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{
					if(Z_TYPE_PP(option_data) != IS_STRING)
						continue;

					zval temp_group_by_column = **option_data;
					zval_copy_ctor(&temp_group_by_column);
					convert_to_string(&temp_group_by_column);

					group_by_columns.push_back(string(Z_STRVAL(temp_group_by_column)));

					zval_dtor(&temp_group_by_column);
				}
			}
			else if(strcmp("aggregate", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{
					char *aggregate_column;
					uint aggregate_column_len;
					uint64_t aggregate_column_index;

					if(zend_hash_get_current_key_ex(option_hash, &aggregate_column, &aggregate_column_len,
								&aggregate_column_index, 0, &option_pointer) != HASH_KEY_IS_STRING)
						continue;

					if(Z_TYPE_PP(option_data) != IS_ARRAY)
						continue;

					string as_name = "";
					string aggregate_type = "";

					zval *aggregate_arr,**aggregate_data;
					HashTable *aggregate_hash;
					HashPosition aggregate_pointer;

					aggregate_arr = *option_data;
					aggregate_hash = Z_ARRVAL_P(aggregate_arr);

					for(zend_hash_internal_pointer_reset_ex(aggregate_hash, &aggregate_pointer);
							zend_hash_get_current_data_ex(aggregate_hash, (void**)&aggregate_data, &aggregate_pointer)==SUCCESS;
							zend_hash_move_forward_ex(aggregate_hash, &aggregate_pointer))
					{
						char *aggregate_param;
						uint aggregate_param_len;
						uint64_t aggregate_param_index;

						if(zend_hash_get_current_key_ex(aggregate_hash, &aggregate_param, &aggregate_param_len,
									&aggregate_param_index, 0, &aggregate_pointer) != HASH_KEY_IS_STRING)
							continue;

						if(Z_TYPE_PP(aggregate_data) != IS_STRING)
							continue;

						zval temp_aggregate_value = **aggregate_data;
						zval_copy_ctor(&temp_aggregate_value);
						convert_to_string(&temp_aggregate_value);

						aggregate_type = string(aggregate_param);
						as_name = string(Z_STRVAL(temp_aggregate_value));

						zval_dtor(&temp_aggregate_value);
					}

					if(aggregate_type.length() > 0 && as_name.length() > 0)
					{
						bool is_push = true;
						if(aggregate_type == "sum")
							aggregate_types.push_back(OB_SUM);
						else if(aggregate_type == "count")
							aggregate_types.push_back(OB_COUNT);
						else if(aggregate_type == "max")
							aggregate_types.push_back(OB_MAX);
						else if(aggregate_type == "min")
							aggregate_types.push_back(OB_MIN);
						else
							is_push = false;
						if(is_push)
						{
							aggregate_columns.push_back(string(aggregate_column));
							as_names.push_back(as_name);
						}
					}

				}
			}
			else if(strcmp("group_by_complex", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{
					char *group_complex_column;
					uint group_complex_column_len;
					uint64_t group_complex_column_index;

					if(zend_hash_get_current_key_ex(option_hash, &group_complex_column, &group_complex_column_len,
								&group_complex_column_index, 0, &option_pointer) != HASH_KEY_IS_STRING)
						continue;        

					if(Z_TYPE_PP(option_data) != IS_STRING)
						continue;

					zval temp_group_complex = **option_data;
					zval_copy_ctor(&temp_group_complex);
					convert_to_string(&temp_group_complex);

					group_by_complex_columns.push_back(string(group_complex_column));
					group_by_complex_as_names.push_back(string(Z_STRVAL(temp_group_complex)));
					zval_dtor(&temp_group_complex);    
				}
			}
			else if(strcmp("having", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{
					if(Z_TYPE_PP(option_data) != IS_STRING)
						continue;

					zval temp_having = **option_data;
					zval_copy_ctor(&temp_having);
					convert_to_string(&temp_having);

					havings.push_back(string(Z_STRVAL(temp_having)));
					zval_dtor(&temp_having);
				}
			}
			else if(strcmp("cond", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{
					//TODO
				}
			}
			else if(strcmp("where", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{
					if(Z_TYPE_PP(option_data) != IS_STRING)
						continue;

					zval temp_where = **option_data;
					zval_copy_ctor(&temp_where);
					convert_to_string(&temp_where);

					wheres.push_back(string(Z_STRVAL(temp_where)));
					zval_dtor(&temp_where);
				}
			}
			else if(strcmp("simple_where", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{   
					if(Z_TYPE_PP(option_data) != IS_STRING)
						continue;

					zval temp_simple_where = **option_data;
					zval_copy_ctor(&temp_simple_where);
					convert_to_string(&temp_simple_where);

					simple_wheres.push_back(string(Z_STRVAL(temp_simple_where)));
					zval_dtor(&temp_simple_where);
				}   
			}

			else if(strcmp("precision", arg_option) == 0)
			{
				//TODO
			}
		}
	}

	const char *arg_row_start = NULL, *arg_row_end = NULL;
	if(row_start.length() > 0)
		arg_row_start = row_start.c_str();
	else
		start_included = 0;
	if(row_end.length() > 0)
		arg_row_end = row_end.c_str();
	else
		end_included = 0;

	ob_scan(ob_scan_st, arg_table, arg_row_start, row_start.length(), start_included,
			arg_row_end, row_end.length(), end_included);

	if(columns.size() > 0)
		for(uint i=0; i<columns.size(); i++)
			ob_scan_column(ob_scan_st, columns[i].c_str(), 1);

	if(wheres.size() > 0)
		for(uint i=0; i<wheres.size(); i++)
			ob_scan_set_where(ob_scan_st, wheres[i].c_str());

	if(simple_wheres.size() > 0)
	{
		map<string,OB_LOGIC_OPERATOR> str_to_op_map;
		str_to_op_map.insert(make_pair("<",OB_LT));
		str_to_op_map.insert(make_pair("<=",OB_LE));
		str_to_op_map.insert(make_pair("=",OB_EQ));
		str_to_op_map.insert(make_pair("!=",OB_NE));
		str_to_op_map.insert(make_pair(">",OB_GT));
		str_to_op_map.insert(make_pair(">=",OB_GE));
		str_to_op_map.insert(make_pair("like",OB_LIKE));
		str_to_op_map.insert(make_pair("before",OB_LT));
		str_to_op_map.insert(make_pair("after",OB_GT));

		//TODO: error deal
		for(uint i=0; i<simple_wheres.size(); i++)
		{
			int start_pos = simple_wheres[i].find(" ");
			int end_pos = simple_wheres[i].rfind(" ");

			string first_str = simple_wheres[i].substr(0,start_pos);
			string second_str = simple_wheres[i].substr(start_pos+1, end_pos-1-start_pos);
			string last_str = simple_wheres[i].substr(end_pos+1, simple_wheres[i].length()-1-end_pos);

			string simple_where_column = first_str;
			OB_LOGIC_OPERATOR simple_where_op = str_to_op_map[second_str];
			OB_VALUE* simple_where_value = (OB_VALUE*)emalloc(sizeof(OB_VALUE));

			//php_printf("%s %s %s\n",simple_where_column.c_str(), second_str.c_str(), last_str.c_str());
			if(simple_where_op == OB_LIKE)
			{
				simple_where_value->type = OB_VARCHAR_TYPE;
				simple_where_value->v.v_varchar.len = last_str.length();
				simple_where_value->v.v_varchar.p = (char *)emalloc((last_str.length()+1)* sizeof(char));
				strcpy(simple_where_value->v.v_varchar.p, last_str.c_str());
				ob_scan_add_simple_where_varchar(ob_scan_st, simple_where_column.c_str(), simple_where_op, simple_where_value->v.v_varchar);
				efree(simple_where_value->v.v_varchar.p);
				simple_where_value->v.v_varchar.p = NULL;
			}
			else if(second_str == "before" || second_str == "after")
			{
				simple_where_value->type = OB_DATETIME_TYPE;
				simple_where_value->v.v_datetime.tv_sec = strtol(last_str.c_str(), NULL, 10)/1000;
				simple_where_value->v.v_datetime.tv_usec = (strtol(last_str.c_str(), NULL, 10) % 1000) * 1000;
				ob_scan_add_simple_where_datetime(ob_scan_st, simple_where_column.c_str(), simple_where_op, simple_where_value->v.v_datetime);
			}
			else 
			{
				simple_where_value->type = OB_INT_TYPE;
				simple_where_value->v.v_int = strtol(last_str.c_str(), NULL, 10);
				ob_scan_add_simple_where_int(ob_scan_st, simple_where_column.c_str(), simple_where_op, simple_where_value->v.v_int);
			}
			efree(simple_where_value);
			simple_where_value = NULL;
		}
	}

	if(limit_offset|limit_count)
		ob_scan_set_limit(ob_scan_st, limit_offset, limit_count);

	if( aggregate_columns.size() > 0)
	{
		OB_GROUPBY_PARAM* ob_group_by_st = ob_get_ob_groupby_param(ob_scan_st);
		if(ob_group_by_st != NULL)
		{
			for(uint i=0; i<group_by_columns.size(); i++)
				ob_groupby_column(ob_group_by_st, group_by_columns[i].c_str(), 1);
			for(uint i=0; i<aggregate_columns.size(); i++)
				ob_aggregate_column(ob_group_by_st, aggregate_types[i], 
						aggregate_columns[i].c_str(), as_names[i].c_str(), 1);
			for(uint i=0; i<group_by_complex_columns.size(); i++)
				ob_groupby_add_complex_column(ob_group_by_st, group_by_complex_columns[i].c_str(),
						group_by_complex_as_names[i].c_str(), 1);
			for(uint i=0; i<havings.size(); i++)
				ob_groupby_set_having(ob_group_by_st, havings[i].c_str());
		}
	}

	if(order_by_columns.size() > 0 && order_by_columns.size() == order_by_types.size())
		for(uint i=0; i<order_by_columns.size(); i++)
			ob_scan_orderby_column(ob_scan_st, order_by_columns[i].c_str(), order_by_types[i]);

	if(ob_errno() != OB_ERR_SUCCESS)
	{
		string err_str = "ob_scan error";
		print_error_info(err_str.c_str() TSRMLS_CC);

		ob_release_scan_st(obj->ob, ob_scan_st);
		ob_scan_st = NULL;
        ob_set_errno(0);	
		RETURN_FALSE;
	}

	OB_RES* ob_res_st = ob_exec_scan(obj->ob, ob_scan_st);
	if(ob_res_st == NULL)
	{
		string err_str = "ob_exec_scan error";
		print_error_info(err_str.c_str() TSRMLS_CC);

		ob_release_scan_st(obj->ob, ob_scan_st);
		ob_scan_st = NULL;
		RETURN_FALSE;
	}

	OB_CELL* cell = NULL;

	array_init(return_value);
	zval *row_key_subarray = NULL;
	string last_row_key_str = "";
	bool start = true;
	while ((cell = ob_fetch_cell(ob_res_st)) != NULL)
	{
		if(cell->is_row_not_exist)
			continue;

		if(cell->is_row_changed)
		{
			if(!start)
				add_assoc_zval_ex(return_value, last_row_key_str.c_str(), last_row_key_str.length(), row_key_subarray);
			else
				start = false;

			zval* temp_row_key;
			row_key_subarray = temp_row_key;
			ALLOC_INIT_ZVAL(row_key_subarray);
			array_init(row_key_subarray);
			last_row_key_str = string(cell->row_key, (int)cell->row_key_len + 1);
		}

		string column_str = string (cell->column, (int)cell->column_len);

		if(cell->is_null)
			add_assoc_null(row_key_subarray, column_str.c_str());
		else if(cell->v.type == OB_INT_TYPE)
			add_assoc_long(row_key_subarray, column_str.c_str(), cell->v.v.v_int);
		else if(cell->v.type == OB_VARCHAR_TYPE)
			add_assoc_stringl(row_key_subarray, column_str.c_str(), cell->v.v.v_varchar.p, (int)cell->v.v.v_varchar.len, 1);
		else if(cell->v.type == OB_DATETIME_TYPE)
			add_assoc_long(row_key_subarray, column_str.c_str(), 1000 * cell->v.v.v_datetime.tv_sec + cell->v.v.v_datetime.tv_usec/1000);
	}
	if (row_key_subarray != NULL)
		add_assoc_zval_ex(return_value, last_row_key_str.c_str(), last_row_key_str.length(), row_key_subarray);

	//TODO: loop for all results 
	ob_release_res_st(obj->ob, ob_res_st);
	ob_res_st = NULL;
	ob_release_scan_st(obj->ob, ob_scan_st);
	ob_scan_st = NULL;

	if(row_key_subarray == NULL)
		RETURN_NULL();
}

PHP_METHOD(OceanBase, insert)
{
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);

	char* arg_table = NULL;
	int arg_table_len;

	char* arg_row_key = NULL;
	int arg_row_key_len;

	zval *columns_arr,**columns_data;
	HashTable *columns_hash;
	HashPosition columns_pointer;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ssa",
				&arg_table, &arg_table_len,
				&arg_row_key, &arg_row_key_len,
				&columns_arr) == FAILURE) {
		RETURN_FALSE;
	}

	OB_SET* ob_set_st = ob_acquire_set_st(obj->ob);
    if (ob_set_st == NULL)
    {
        string err_str = "ob_acquire_set_st error";
        print_error_info(err_str.c_str() TSRMLS_CC);
        RETURN_FALSE;
    }

	columns_hash = Z_ARRVAL_P(columns_arr);

	for(zend_hash_internal_pointer_reset_ex(columns_hash, &columns_pointer);
			zend_hash_get_current_data_ex(columns_hash, (void**)&columns_data, &columns_pointer)==SUCCESS;
			zend_hash_move_forward_ex(columns_hash, &columns_pointer))
	{
		char *arg_column;
		uint arg_column_len;
		uint64_t arg_column_index;

		if(zend_hash_get_current_key_ex(columns_hash, &arg_column, &arg_column_len,
					&arg_column_index, 0, &columns_pointer) != HASH_KEY_IS_STRING)
			continue;

		if(Z_TYPE_PP(columns_data) != IS_ARRAY)
			continue;

		zval *column_arr, **column_data;
		HashTable *column_hash;
		HashPosition column_pointer;

		column_arr = *columns_data;
		column_hash = Z_ARRVAL_P(column_arr);

		zval column_value;
		string column_value_type;
		bool should_destruct = false;

		for(zend_hash_internal_pointer_reset_ex(column_hash, &column_pointer);
				zend_hash_get_current_data_ex(column_hash, (void**)&column_data, &column_pointer)==SUCCESS;
				zend_hash_move_forward_ex(column_hash, &column_pointer))
		{
			char *arg_param;
			uint arg_param_len;
			uint64_t arg_param_index;

			if(zend_hash_get_current_key_ex(column_hash, &arg_param, &arg_param_len,
						&arg_param_index, 0, &column_pointer) != HASH_KEY_IS_STRING)
				continue;

			column_value_type = string(arg_param);
			column_value = **column_data;
			zval_copy_ctor(&column_value);
			should_destruct = true;
		}

		if(should_destruct)
		{		
			if(column_value_type == "int" && Z_TYPE(column_value) == IS_LONG)
			{
				convert_to_long(&column_value);
				ob_insert_int(ob_set_st, arg_table, arg_row_key, arg_row_key_len, arg_column, Z_LVAL(column_value));
			}
			else if(column_value_type == "varchar" && Z_TYPE(column_value) == IS_STRING)
			{
				convert_to_string(&column_value);
				OB_VARCHAR value_char;
				value_char.p = (char *)emalloc((Z_STRLEN(column_value)+1) * sizeof(char));
				value_char.len = Z_STRLEN(column_value);
				strcpy(value_char.p,Z_STRVAL(column_value));
				ob_insert_varchar(ob_set_st, arg_table, arg_row_key, arg_row_key_len, arg_column, value_char);
				efree(value_char.p);
				value_char.p = NULL;
			}
			else if(column_value_type == "datetime" && Z_TYPE(column_value) == IS_LONG)
			{
				convert_to_long(&column_value);
				OB_DATETIME value_date;
				value_date.tv_sec = Z_LVAL(column_value)/1000;
				value_date.tv_usec = (Z_LVAL(column_value) % 1000) * 1000;
				ob_insert_datetime(ob_set_st, arg_table, arg_row_key, arg_row_key_len, arg_column, value_date);
			}
			zval_dtor(&column_value);
		}
	}

	if(ob_errno() != OB_ERR_SUCCESS)
	{
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_insert error";
		print_error_info(err_str.c_str() TSRMLS_CC);
        ob_set_errno(0);
		RETURN_FALSE;
	}

	OB_ERR_CODE err = ob_exec_set(obj->ob, ob_set_st);
	if(err != OB_ERR_SUCCESS)
	{
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_exec_insert error";
		print_error_info(err_str.c_str() TSRMLS_CC);
		RETURN_FALSE;
	}

	ob_release_set_st(obj->ob, ob_set_st);
	ob_set_st = NULL;

	RETURN_TRUE;
}

PHP_METHOD(OceanBase, minsert)
{
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);

	zval *table_arr,**table_data;
	HashTable *table_hash;
	HashPosition table_pointer;
	char *table_name;
	uint table_name_len;
	zval *key_arr;

	if(zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "a", &table_arr) == SUCCESS ) {
		;
	}
	else if(zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "sa",
				&table_name, &table_name_len, &key_arr) == SUCCESS ) {
		ALLOC_INIT_ZVAL(table_arr);
		array_init(table_arr);
		add_assoc_zval(table_arr, table_name, key_arr);
	}
	else {
		php_error(E_WARNING, "arguments error\n");
		RETURN_FALSE;
	}

	OB_SET* ob_set_st = ob_acquire_set_st(obj->ob);
    if (ob_set_st == NULL)
    {
        string err_str = "ob_acquire_set_st error";
        print_error_info(err_str.c_str() TSRMLS_CC);
        RETURN_FALSE;
    }

	table_hash = Z_ARRVAL_P(table_arr);

	for(zend_hash_internal_pointer_reset_ex(table_hash, &table_pointer);
			zend_hash_get_current_data_ex(table_hash, (void**)&table_data, &table_pointer)==SUCCESS;
			zend_hash_move_forward_ex(table_hash, &table_pointer))
	{
		char *arg_table;
		uint arg_table_len;
		uint64_t arg_table_index;

		if(zend_hash_get_current_key_ex(table_hash, &arg_table, &arg_table_len,
					&arg_table_index, 0, &table_pointer) != HASH_KEY_IS_STRING)
			continue;

		if(Z_TYPE_PP(table_data) != IS_ARRAY)
			continue;

		zval *row_key_arr,**row_key_data;
		HashTable *row_key_hash;
		HashPosition row_key_pointer;

		row_key_arr = *table_data;
		row_key_hash = Z_ARRVAL_P(row_key_arr);

		for(zend_hash_internal_pointer_reset_ex(row_key_hash, &row_key_pointer);
				zend_hash_get_current_data_ex(row_key_hash, (void**)&row_key_data, &row_key_pointer)==SUCCESS;
				zend_hash_move_forward_ex(row_key_hash, &row_key_pointer))
		{
			char *arg_row_key;
			uint arg_row_key_len;
			uint64_t arg_row_key_index;

			if(zend_hash_get_current_key_ex(row_key_hash, &arg_row_key, &arg_row_key_len, 
						&arg_row_key_index, 0, &row_key_pointer) != HASH_KEY_IS_STRING)
				continue;

			if(Z_TYPE_PP(row_key_data) != IS_ARRAY)
				continue;

			zval *columns_arr,**columns_data;
			HashTable *columns_hash;
			HashPosition columns_pointer;

			columns_arr = *row_key_data;
			columns_hash = Z_ARRVAL_P(columns_arr);

			for(zend_hash_internal_pointer_reset_ex(columns_hash, &columns_pointer);
					zend_hash_get_current_data_ex(columns_hash, (void**)&columns_data, &columns_pointer)==SUCCESS;
					zend_hash_move_forward_ex(columns_hash, &columns_pointer))
			{
				char *arg_column;
				uint arg_column_len;
				uint64_t arg_column_index;

				if(zend_hash_get_current_key_ex(columns_hash, &arg_column, &arg_column_len,
							&arg_column_index, 0, &columns_pointer) != HASH_KEY_IS_STRING)
					continue;

				if(Z_TYPE_PP(columns_data) != IS_ARRAY)
					continue;

				zval *column_arr, **column_data;
				HashTable *column_hash;
				HashPosition column_pointer;

				column_arr = *columns_data;
				column_hash = Z_ARRVAL_P(column_arr);

				zval column_value;
				string column_value_type;
				bool should_destruct = false;

				for(zend_hash_internal_pointer_reset_ex(column_hash, &column_pointer);
						zend_hash_get_current_data_ex(column_hash, (void**)&column_data, &column_pointer)==SUCCESS;
						zend_hash_move_forward_ex(column_hash, &column_pointer))
				{
					char *arg_param;
					uint arg_param_len;
					uint64_t arg_param_index;

					if(zend_hash_get_current_key_ex(column_hash, &arg_param, &arg_param_len,
								&arg_param_index, 0, &column_pointer) != HASH_KEY_IS_STRING)
						continue;

					column_value_type = string(arg_param);
					column_value = **column_data;
					zval_copy_ctor(&column_value);
					should_destruct = true;
				}

				if(should_destruct)
				{		
					if(column_value_type == "int" && Z_TYPE(column_value) == IS_LONG)
					{
						convert_to_long(&column_value);
						ob_insert_int(ob_set_st, arg_table, arg_row_key, arg_row_key_len - 1, arg_column, Z_LVAL(column_value));
					}
					else if(column_value_type == "varchar" && Z_TYPE(column_value) == IS_STRING)
					{
						convert_to_string(&column_value);
						OB_VARCHAR value_char;
						value_char.p = (char *)emalloc((Z_STRLEN(column_value)+1) * sizeof(char));
						value_char.len = Z_STRLEN(column_value);
						strcpy(value_char.p,Z_STRVAL(column_value));
						ob_insert_varchar(ob_set_st, arg_table, arg_row_key, arg_row_key_len - 1, arg_column, value_char);
						efree(value_char.p);
						value_char.p = NULL;
					}
					else if(column_value_type == "datetime" && Z_TYPE(column_value) == IS_LONG)
					{
						convert_to_long(&column_value);
						OB_DATETIME value_date;
						value_date.tv_sec = Z_LVAL(column_value)/1000;
						value_date.tv_usec = (Z_LVAL(column_value) % 1000) * 1000;
						ob_insert_datetime(ob_set_st, arg_table, arg_row_key, arg_row_key_len - 1, arg_column, value_date);
					}
					zval_dtor(&column_value);
				}
			}
		}
	}

	if(ob_errno() != OB_ERR_SUCCESS)
	{	
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_minsert error";
		print_error_info(err_str.c_str() TSRMLS_CC);
        ob_set_errno(0);
		RETURN_FALSE;
	}

	OB_ERR_CODE err = ob_exec_set(obj->ob, ob_set_st);
	if(err != OB_ERR_SUCCESS)
	{
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_exec_minsert error";
		print_error_info(err_str.c_str() TSRMLS_CC);
		RETURN_FALSE;
	}

	ob_release_set_st(obj->ob, ob_set_st);
	ob_set_st = NULL;

	RETURN_TRUE;
}

PHP_METHOD(OceanBase, update)
{
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);

	char* arg_table = NULL;
	int arg_table_len;

	char* arg_row_key = NULL;
	int arg_row_key_len;

	zval *columns_arr,**columns_data;
	HashTable *columns_hash;
	HashPosition columns_pointer;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ssa",
				&arg_table, &arg_table_len,
				&arg_row_key, &arg_row_key_len,
				&columns_arr) == FAILURE) {
		RETURN_FALSE;
	}

	OB_SET* ob_set_st = ob_acquire_set_st(obj->ob);
    if (ob_set_st == NULL)
    {
        string err_str = "ob_acquire_set_st error";
        print_error_info(err_str.c_str() TSRMLS_CC);
        RETURN_FALSE;
    }

	columns_hash = Z_ARRVAL_P(columns_arr);

	for(zend_hash_internal_pointer_reset_ex(columns_hash, &columns_pointer);
			zend_hash_get_current_data_ex(columns_hash, (void**)&columns_data, &columns_pointer)==SUCCESS;
			zend_hash_move_forward_ex(columns_hash, &columns_pointer))
	{
		char *arg_column;
		uint arg_column_len;
		uint64_t arg_column_index;

		if(zend_hash_get_current_key_ex(columns_hash, &arg_column, &arg_column_len,
					&arg_column_index, 0, &columns_pointer) != HASH_KEY_IS_STRING)
			continue;

		if(Z_TYPE_PP(columns_data) != IS_ARRAY)
			continue;

		zval *column_arr, **column_data;
		HashTable *column_hash;
		HashPosition column_pointer;

		column_arr = *columns_data;
		column_hash = Z_ARRVAL_P(column_arr);

		zval column_value;
		string column_value_type;
		bool should_destruct = false;

		for(zend_hash_internal_pointer_reset_ex(column_hash, &column_pointer);
				zend_hash_get_current_data_ex(column_hash, (void**)&column_data, &column_pointer)==SUCCESS;
				zend_hash_move_forward_ex(column_hash, &column_pointer))
		{
			char *arg_param;
			uint arg_param_len;
			uint64_t arg_param_index;

			if(zend_hash_get_current_key_ex(column_hash, &arg_param, &arg_param_len,
						&arg_param_index, 0, &column_pointer) != HASH_KEY_IS_STRING)
				continue;

			column_value_type = string(arg_param);
			column_value = **column_data;
			zval_copy_ctor(&column_value);
			should_destruct = true;
		}

		if(should_destruct)
		{		
			if(column_value_type == "int" && Z_TYPE(column_value) == IS_LONG)
			{
				convert_to_long(&column_value);
				ob_update_int(ob_set_st, arg_table, arg_row_key, arg_row_key_len, arg_column, Z_LVAL(column_value));
			}
			else if(column_value_type == "varchar" && Z_TYPE(column_value) == IS_STRING)
			{
				convert_to_string(&column_value);
				OB_VARCHAR value_char;
				value_char.p = (char *)emalloc((Z_STRLEN(column_value)+1) * sizeof(char));
				value_char.len = Z_STRLEN(column_value);
				strcpy(value_char.p,Z_STRVAL(column_value));
				ob_update_varchar(ob_set_st, arg_table, arg_row_key, arg_row_key_len, arg_column, value_char);
				efree(value_char.p);
				value_char.p = NULL;
			}
			else if(column_value_type == "datetime" && Z_TYPE(column_value) == IS_LONG)
			{
				convert_to_long(&column_value);
				OB_DATETIME value_date;
				value_date.tv_sec = Z_LVAL(column_value)/1000;
				value_date.tv_usec = (Z_LVAL(column_value) % 1000) * 1000;
				ob_update_datetime(ob_set_st, arg_table, arg_row_key, arg_row_key_len, arg_column, value_date);
			}
			zval_dtor(&column_value);
		}
	}

	if(ob_errno() != OB_ERR_SUCCESS)
	{
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_update error";
		print_error_info(err_str.c_str() TSRMLS_CC);
        ob_set_errno(0);
		RETURN_FALSE;
	}

	OB_ERR_CODE err = ob_exec_set(obj->ob, ob_set_st);
	if(err != OB_ERR_SUCCESS)
	{
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_exec_update error";
		print_error_info(err_str.c_str() TSRMLS_CC);
		RETURN_FALSE;
	}

	ob_release_set_st(obj->ob, ob_set_st);
	ob_set_st = NULL;

	RETURN_TRUE;
}

PHP_METHOD(OceanBase, mupdate)
{
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);

	zval *table_arr,**table_data;
	HashTable *table_hash;
	HashPosition table_pointer;

	char *table_name;
	uint table_name_len;
	zval *key_arr;

	if(zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "a", &table_arr) == SUCCESS ) {
		;
	}   
	else if(zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "sa",
				&table_name, &table_name_len, &key_arr) == SUCCESS ) {
		ALLOC_INIT_ZVAL(table_arr);
		array_init(table_arr);
		add_assoc_zval(table_arr, table_name, key_arr);
	}   
	else {
		php_error(E_WARNING, "arguments error\n");
		RETURN_FALSE;
	}

	OB_SET* ob_set_st = ob_acquire_set_st(obj->ob);
    if (ob_set_st == NULL)
    {
        string err_str = "ob_acquire_set_st error";
        print_error_info(err_str.c_str() TSRMLS_CC);
        RETURN_FALSE;
    }

	table_hash = Z_ARRVAL_P(table_arr);

	for(zend_hash_internal_pointer_reset_ex(table_hash, &table_pointer);
			zend_hash_get_current_data_ex(table_hash, (void**)&table_data, &table_pointer)==SUCCESS;
			zend_hash_move_forward_ex(table_hash, &table_pointer))
	{
		char *arg_table;
		uint arg_table_len;
		uint64_t arg_table_index;

		if(zend_hash_get_current_key_ex(table_hash, &arg_table, &arg_table_len,
					&arg_table_index, 0, &table_pointer) != HASH_KEY_IS_STRING)
			continue;

		if(Z_TYPE_PP(table_data) != IS_ARRAY)
			continue;

		zval *row_key_arr,**row_key_data;
		HashTable *row_key_hash;
		HashPosition row_key_pointer;

		row_key_arr = *table_data;
		row_key_hash = Z_ARRVAL_P(row_key_arr);

		for(zend_hash_internal_pointer_reset_ex(row_key_hash, &row_key_pointer);
				zend_hash_get_current_data_ex(row_key_hash, (void**)&row_key_data, &row_key_pointer)==SUCCESS;
				zend_hash_move_forward_ex(row_key_hash, &row_key_pointer))
		{
			char *arg_row_key;
			uint arg_row_key_len;
			uint64_t arg_row_key_index;

			if(zend_hash_get_current_key_ex(row_key_hash, &arg_row_key, &arg_row_key_len, 
						&arg_row_key_index, 0, &row_key_pointer) != HASH_KEY_IS_STRING)
				continue;

			if(Z_TYPE_PP(row_key_data) != IS_ARRAY)
				continue;

			zval *columns_arr,**columns_data;
			HashTable *columns_hash;
			HashPosition columns_pointer;

			columns_arr = *row_key_data;
			columns_hash = Z_ARRVAL_P(columns_arr);

			for(zend_hash_internal_pointer_reset_ex(columns_hash, &columns_pointer);
					zend_hash_get_current_data_ex(columns_hash, (void**)&columns_data, &columns_pointer)==SUCCESS;
					zend_hash_move_forward_ex(columns_hash, &columns_pointer))
			{
				char *arg_column;
				uint arg_column_len;
				uint64_t arg_column_index;

				if(zend_hash_get_current_key_ex(columns_hash, &arg_column, &arg_column_len,
							&arg_column_index, 0, &columns_pointer) != HASH_KEY_IS_STRING)
					continue;

				if(Z_TYPE_PP(columns_data) != IS_ARRAY)
					continue;

				zval *column_arr, **column_data;
				HashTable *column_hash;
				HashPosition column_pointer;

				column_arr = *columns_data;
				column_hash = Z_ARRVAL_P(column_arr);

				zval column_value;
				string column_value_type;
				bool should_destruct = false;

				for(zend_hash_internal_pointer_reset_ex(column_hash, &column_pointer);
						zend_hash_get_current_data_ex(column_hash, (void**)&column_data, &column_pointer)==SUCCESS;
						zend_hash_move_forward_ex(column_hash, &column_pointer))
				{
					char *arg_param;
					uint arg_param_len;
					uint64_t arg_param_index;

					if(zend_hash_get_current_key_ex(column_hash, &arg_param, &arg_param_len,
								&arg_param_index, 0, &column_pointer) != HASH_KEY_IS_STRING)
						continue;

					column_value_type = string(arg_param);
					column_value = **column_data;
					zval_copy_ctor(&column_value);
					should_destruct = true;
				}

				if(should_destruct)
				{		
					if(column_value_type == "int" && Z_TYPE(column_value) == IS_LONG)
					{
						convert_to_long(&column_value);
						ob_update_int(ob_set_st, arg_table, arg_row_key, arg_row_key_len - 1, arg_column, Z_LVAL(column_value));
					}
					else if(column_value_type == "varchar" && Z_TYPE(column_value) == IS_STRING)
					{
						convert_to_string(&column_value);
						OB_VARCHAR value_char;
						value_char.p = (char *)emalloc((Z_STRLEN(column_value)+1) * sizeof(char));
						value_char.len = Z_STRLEN(column_value);
						strcpy(value_char.p,Z_STRVAL(column_value));
						ob_update_varchar(ob_set_st, arg_table, arg_row_key, arg_row_key_len - 1, arg_column, value_char);
						efree(value_char.p);
						value_char.p = NULL;
					}
					else if(column_value_type == "datetime" && Z_TYPE(column_value) == IS_LONG)
					{
						convert_to_long(&column_value);
						OB_DATETIME value_date;
						value_date.tv_sec = Z_LVAL(column_value)/1000;
						value_date.tv_usec = (Z_LVAL(column_value) % 1000) * 1000;
						ob_update_datetime(ob_set_st, arg_table, arg_row_key, arg_row_key_len - 1, arg_column, value_date);
					}
					zval_dtor(&column_value);
				}
			}
		}
	}

	if(ob_errno() != OB_ERR_SUCCESS)
	{
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_mupdate error";
		print_error_info(err_str.c_str() TSRMLS_CC);
        ob_set_errno(0);
		RETURN_FALSE;
	}

	OB_ERR_CODE err = ob_exec_set(obj->ob, ob_set_st);
	if(err != OB_ERR_SUCCESS)
	{
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_exec_mupdate error";
		print_error_info(err_str.c_str() TSRMLS_CC);
		RETURN_FALSE;
	}

	ob_release_set_st(obj->ob, ob_set_st);
	ob_set_st = NULL;

	RETURN_TRUE;
}

PHP_METHOD(OceanBase, delete)
{
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);

	char* arg_table = NULL;
	int arg_table_len;

	zval *row_keys_arr,**row_keys_data;
	HashTable *row_keys_hash;
	HashPosition row_keys_pointer;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sa", 
				&arg_table, &arg_table_len, 
				&row_keys_arr) == FAILURE) {
		RETURN_FALSE;
	}

	OB_SET* ob_set_st = ob_acquire_set_st(obj->ob);
    if (ob_set_st == NULL)
    {
        string err_str = "ob_acquire_set_st error";
        print_error_info(err_str.c_str() TSRMLS_CC);
        RETURN_FALSE;
    }

	row_keys_hash = Z_ARRVAL_P(row_keys_arr);

	for(zend_hash_internal_pointer_reset_ex(row_keys_hash, &row_keys_pointer);
			zend_hash_get_current_data_ex(row_keys_hash, (void**)&row_keys_data, &row_keys_pointer)==SUCCESS;
			zend_hash_move_forward_ex(row_keys_hash, &row_keys_pointer))
	{
		if(Z_TYPE_PP(row_keys_data) != IS_STRING)
			continue;	

		zval arg_row_key = **row_keys_data;
		zval_copy_ctor(&arg_row_key);
		convert_to_string(&arg_row_key);

		ob_del_row(ob_set_st, arg_table, Z_STRVAL(arg_row_key), Z_STRLEN(arg_row_key));

		zval_dtor(&arg_row_key);
	}

	if(ob_errno() != OB_ERR_SUCCESS)
	{
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_del_row error";
		print_error_info(err_str.c_str() TSRMLS_CC);
        ob_set_errno(0);
		RETURN_FALSE;
	}

	OB_ERR_CODE err = ob_exec_set(obj->ob, ob_set_st);
	if(err != OB_ERR_SUCCESS)
	{
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_exec_delete_error";
		print_error_info(err_str.c_str() TSRMLS_CC);
		RETURN_FALSE;
	}

	ob_release_set_st(obj->ob, ob_set_st);
	ob_set_st = NULL;

	RETURN_TRUE;
}

PHP_METHOD(OceanBase, mdelete)
{
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);

	zval *table_arr,**table_data;
	HashTable *table_hash;
	HashPosition table_pointer;

	if(zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "a", &table_arr) == FAILURE ) {
		RETURN_FALSE;
	}

	OB_SET* ob_set_st = ob_acquire_set_st(obj->ob);
    if (ob_set_st == NULL)
    {
        string err_str = "ob_acquire_set_st error";
        print_error_info(err_str.c_str() TSRMLS_CC);
        RETURN_FALSE;
    }

	table_hash = Z_ARRVAL_P(table_arr);

	for(zend_hash_internal_pointer_reset_ex(table_hash, &table_pointer);
			zend_hash_get_current_data_ex(table_hash, (void**)&table_data, &table_pointer)==SUCCESS;
			zend_hash_move_forward_ex(table_hash, &table_pointer) )
	{
		char* arg_table;
		uint arg_table_len;
		uint64_t arg_table_index;

		if(zend_hash_get_current_key_ex(table_hash, &arg_table, &arg_table_len,
					&arg_table_index, 0, &table_pointer) != HASH_KEY_IS_STRING)
			continue;

		if(Z_TYPE_PP(table_data) != IS_ARRAY)
			continue;

		zval *row_keys_arr,**row_keys_data;
		HashTable *row_keys_hash;
		HashPosition row_keys_pointer;

		row_keys_arr = *table_data;
		row_keys_hash = Z_ARRVAL_P(row_keys_arr);

		for(zend_hash_internal_pointer_reset_ex(row_keys_hash, &row_keys_pointer);
				zend_hash_get_current_data_ex(row_keys_hash, (void**)&row_keys_data, &row_keys_pointer)==SUCCESS;
				zend_hash_move_forward_ex(row_keys_hash, &row_keys_pointer))
		{
			if(Z_TYPE_PP(row_keys_data) != IS_STRING)
				continue;

			zval arg_row_key = **row_keys_data;
			zval_copy_ctor(&arg_row_key);
			convert_to_string(&arg_row_key);

			ob_del_row(ob_set_st, arg_table, Z_STRVAL(arg_row_key), Z_STRLEN(arg_row_key));

			zval_dtor(&arg_row_key);
		}	
	}

	if(ob_errno() != OB_ERR_SUCCESS)
	{
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_del_row error";
		print_error_info(err_str.c_str() TSRMLS_CC);
        ob_set_errno(0);
		RETURN_FALSE;
	}

	OB_ERR_CODE err = ob_exec_set(obj->ob, ob_set_st);
	if(err != OB_ERR_SUCCESS)
	{   
		ob_release_set_st(obj->ob, ob_set_st);
		ob_set_st = NULL;

		string err_str = "ob_del_row error";
		print_error_info(err_str.c_str() TSRMLS_CC);
		RETURN_FALSE;
	}

	ob_release_set_st(obj->ob, ob_set_st);
	ob_set_st = NULL;

	RETURN_TRUE;
}


PHP_METHOD(OceanBase, fetchRowNum)
{
	zval *self = getThis();
	oceanbase_object *obj = (oceanbase_object*)zend_object_store_get_object(self TSRMLS_CC);

	char* arg_table = NULL;
	int arg_table_len = 0;

	zval *options_arr = NULL,**options_data;
	HashTable *options_hash;
	HashPosition options_pointer;
	
	if(zend_parse_parameters_ex(ZEND_PARSE_PARAMS_QUIET, ZEND_NUM_ARGS() TSRMLS_CC, "s|a", 
				&arg_table, &arg_table_len,
				&options_arr) == FAILURE ) {
		php_error(E_WARNING, "arguments error\n");
		RETURN_FALSE;
	}

	OB_SCAN* ob_scan_st = ob_acquire_scan_st(obj->ob);
	if(ob_scan_st == NULL)
	{
		string err_str = "ob_acquire_scan_st error";
		print_error_info(err_str.c_str() TSRMLS_CC);
		RETURN_FALSE;
	}

	string row_start = "", row_end = "";
	int64_t start_included = 1, end_included = 1;

	vector<string> wheres;
	vector<string> simple_wheres;

	if(options_arr != NULL)
	{
		options_hash = Z_ARRVAL_P(options_arr);
		for(zend_hash_internal_pointer_reset_ex(options_hash, &options_pointer);
				zend_hash_get_current_data_ex(options_hash, (void**)&options_data, &options_pointer)==SUCCESS;
				zend_hash_move_forward_ex(options_hash, &options_pointer))
		{
			char *arg_option;
			uint arg_option_len;
			uint64_t arg_option_index;

			if(zend_hash_get_current_key_ex(options_hash, &arg_option, &arg_option_len,
						&arg_option_index, 0, &options_pointer) != HASH_KEY_IS_STRING)
			continue;

			if(Z_TYPE_PP(options_data) != IS_ARRAY)
				continue;
			//TODO: precision is not array

			zval *option_arr,**option_data;
			HashTable *option_hash;
			HashPosition option_pointer;

			option_arr = *options_data;
			option_hash = Z_ARRVAL_P(option_arr);

			if(strcmp("range", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{
					char *range_type;
					uint range_type_len;
					uint64_t range_type_index;

					if(zend_hash_get_current_key_ex(option_hash, &range_type, &range_type_len,
								&range_type_index, 0, &option_pointer) != HASH_KEY_IS_STRING)
						continue;

					if(Z_TYPE_PP(option_data) != IS_ARRAY)
						continue;

					zval *range_arr,**range_data;
					HashTable *range_hash;
					HashPosition range_pointer;

					range_arr = *option_data;
					range_hash = Z_ARRVAL_P(range_arr);

					for(zend_hash_internal_pointer_reset_ex(range_hash, &range_pointer);
							zend_hash_get_current_data_ex(range_hash, (void**)&range_data, &range_pointer)==SUCCESS;
							zend_hash_move_forward_ex(range_hash, &range_pointer))
					{

						char *range_key;
						uint range_key_len;
						uint64_t range_key_index;

						if(zend_hash_get_current_key_ex(range_hash, &range_key, &range_key_len,
									&range_key_index, 0, &range_pointer) != HASH_KEY_IS_STRING)
							continue;

						zval temp_val;

						if(strcmp("row_key", range_key) == 0)
						{
							if(Z_TYPE_PP(range_data) != IS_STRING)
								continue;

							if(strcmp("start", range_type) ==0)
							{
								temp_val = **range_data;
								zval_copy_ctor(&temp_val);
								convert_to_string(&temp_val);

								row_start = string(Z_STRVAL(temp_val),Z_STRLEN(temp_val));

								zval_dtor(&temp_val);
							}
							else if(strcmp("end", range_type) ==0)
							{
								temp_val = **range_data;
								zval_copy_ctor(&temp_val);
								convert_to_string(&temp_val);

								row_end = string(Z_STRVAL(temp_val),Z_STRLEN(temp_val));

								zval_dtor(&temp_val);
							}
						}
						else if(strcmp("included", range_key) == 0)
						{
							if(Z_TYPE_PP(range_data) != IS_LONG)
								continue;

							if(strcmp("start", range_type) ==0)
							{
								temp_val = **range_data;
								zval_copy_ctor(&temp_val);
								convert_to_long(&temp_val);

								start_included = Z_LVAL(temp_val);

								zval_dtor(&temp_val);
							}
							else if(strcmp("end", range_type) ==0)
							{
								temp_val = **range_data;
								zval_copy_ctor(&temp_val);
								convert_to_long(&temp_val);

								end_included = Z_LVAL(temp_val);

								zval_dtor(&temp_val);
							}
						}

					}
				}
			}
			else if(strcmp("where", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{
					if(Z_TYPE_PP(option_data) != IS_STRING)
						continue;

					zval temp_where = **option_data;
					zval_copy_ctor(&temp_where);
					convert_to_string(&temp_where);

					wheres.push_back(string(Z_STRVAL(temp_where)));
					zval_dtor(&temp_where);
				}
			}
			else if(strcmp("simple_where", arg_option) == 0)
			{
				for(zend_hash_internal_pointer_reset_ex(option_hash, &option_pointer);
						zend_hash_get_current_data_ex(option_hash, (void**)&option_data, &option_pointer)==SUCCESS;
						zend_hash_move_forward_ex(option_hash, &option_pointer))
				{   
					if(Z_TYPE_PP(option_data) != IS_STRING)
						continue;

					zval temp_simple_where = **option_data;
					zval_copy_ctor(&temp_simple_where);
					convert_to_string(&temp_simple_where);

					simple_wheres.push_back(string(Z_STRVAL(temp_simple_where)));
					zval_dtor(&temp_simple_where);
				}   
			}
		}
	}

	const char *arg_row_start = NULL, *arg_row_end = NULL;
	if(row_start.length() > 0)
		arg_row_start = row_start.c_str();
	else
		start_included = 0;
	if(row_end.length() > 0)
		arg_row_end = row_end.c_str();
	else
		end_included = 0;

	ob_scan(ob_scan_st, arg_table, arg_row_start, row_start.length(), start_included,
			arg_row_end, row_end.length(), end_included);

	if(wheres.size() > 0)
		for(uint i=0; i<wheres.size(); i++)
			ob_scan_set_where(ob_scan_st, wheres[i].c_str());

	if(simple_wheres.size() > 0)
	{
		map<string,OB_LOGIC_OPERATOR> str_to_op_map;
		str_to_op_map.insert(make_pair("<",OB_LT));
		str_to_op_map.insert(make_pair("<=",OB_LE));
		str_to_op_map.insert(make_pair("=",OB_EQ));
		str_to_op_map.insert(make_pair("!=",OB_NE));
		str_to_op_map.insert(make_pair(">",OB_GT));
		str_to_op_map.insert(make_pair(">=",OB_GE));
		str_to_op_map.insert(make_pair("like",OB_LIKE));
		str_to_op_map.insert(make_pair("before",OB_LT));
		str_to_op_map.insert(make_pair("after",OB_GT));

		//TODO: error deal
		for(uint i=0; i<simple_wheres.size(); i++)
		{
			int start_pos = simple_wheres[i].find(" ");
			int end_pos = simple_wheres[i].rfind(" ");

			string first_str = simple_wheres[i].substr(0,start_pos);
			string second_str = simple_wheres[i].substr(start_pos+1, end_pos-1-start_pos);
			string last_str = simple_wheres[i].substr(end_pos+1, simple_wheres[i].length()-1-end_pos);

			string simple_where_column = first_str;
			OB_LOGIC_OPERATOR simple_where_op = str_to_op_map[second_str];
			OB_VALUE* simple_where_value = (OB_VALUE*)emalloc(sizeof(OB_VALUE));

			//php_printf("%s %s %s\n",simple_where_column.c_str(), second_str.c_str(), last_str.c_str());
			if(simple_where_op == OB_LIKE)
			{
				simple_where_value->type = OB_VARCHAR_TYPE;
				simple_where_value->v.v_varchar.len = last_str.length();
				simple_where_value->v.v_varchar.p = (char *)emalloc((last_str.length()+1)* sizeof(char));
				strcpy(simple_where_value->v.v_varchar.p, last_str.c_str());
				ob_scan_add_simple_where_varchar(ob_scan_st, simple_where_column.c_str(), simple_where_op, simple_where_value->v.v_varchar);
				efree(simple_where_value->v.v_varchar.p);
				simple_where_value->v.v_varchar.p = NULL;
			}
			else if(second_str == "before" || second_str == "after")
			{
				simple_where_value->type = OB_DATETIME_TYPE;
				simple_where_value->v.v_datetime.tv_sec = strtol(last_str.c_str(), NULL, 10)/1000;
				simple_where_value->v.v_datetime.tv_usec = (strtol(last_str.c_str(), NULL, 10) % 1000) * 1000;
				ob_scan_add_simple_where_datetime(ob_scan_st, simple_where_column.c_str(), simple_where_op, simple_where_value->v.v_datetime);
			}
			else 
			{
				simple_where_value->type = OB_INT_TYPE;
				simple_where_value->v.v_int = strtol(last_str.c_str(), NULL, 10);
				ob_scan_add_simple_where_int(ob_scan_st, simple_where_column.c_str(), simple_where_op, simple_where_value->v.v_int);
			}
			efree(simple_where_value);
			simple_where_value = NULL;
		}
	}

	if(ob_errno() != OB_ERR_SUCCESS)
	{
		string err_str = "ob_scan error";
		print_error_info(err_str.c_str() TSRMLS_CC);

		ob_release_scan_st(obj->ob, ob_scan_st);
		ob_scan_st = NULL;	
        ob_set_errno(0);
		RETURN_FALSE;
	}

	OB_RES* ob_res_st = ob_exec_scan(obj->ob, ob_scan_st);
	if(ob_res_st == NULL)
	{
		string err_str = "ob_exec_scan error";
		print_error_info(err_str.c_str() TSRMLS_CC);

		ob_release_scan_st(obj->ob, ob_scan_st);
		ob_scan_st = NULL;
		RETURN_FALSE;
	}

	OB_CELL* cell = NULL;

	array_init(return_value);
	int64_t row_num;
	row_num = ob_fetch_row_num(ob_res_st);
	add_assoc_long(return_value, "total", row_num);
	
	ob_release_res_st(obj->ob, ob_res_st);
	ob_res_st = NULL;
	ob_release_scan_st(obj->ob, ob_scan_st);
	ob_scan_st = NULL;

}


/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
