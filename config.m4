dnl $Id$
dnl config.m4 for extension oceanbase

PHP_ARG_WITH(oceanbase, for oceanbase support,
[  --with-oceanbase=[PATH]   Path for oceanbase support])

if test "$PHP_OCEANBASE" != "no"; then
   PHP_REQUIRE_CXX()
   PHP_ADD_LIBRARY(stdc++,"",EXTRA_LDFLAGS)
   SEARCH_PATH="/usr/local/include /usr/include"     # you might want to change this
   SEARCH_FOR="oceanbase.h"  # you most likely want to change this
   LIBNAME=obapi
   
   if test -r $PHP_OCEANBASE/$SEARCH_FOR; then # path given as parameter
     OCEANBASE_DIR=$PHP_OCEANBASE
     PHP_ADD_INCLUDE($OCEANBASE_DIR)
     PHP_ADD_LIBRARY_WITH_PATH($LIBNAME, $OCEANBASE_DIR, OCEANBASE_SHARED_LIBADD)
   else # search default path list
     AC_MSG_CHECKING([for oceanbase files in default path])
     for i in $SEARCH_PATH ; do
       if test -r $i/$SEARCH_FOR; then
         OCEANBASE_DIR=$i
         AC_MSG_RESULT(found in $i)
       fi
     done
     PHP_ADD_LIBRARY($LIBNAME,,OCEANBASE_SHARED_LIBADD)
   fi
  
   if test -z "$OCEANBASE_DIR"; then
     AC_MSG_RESULT([not found])
     AC_MSG_ERROR([Please reinstall the oceanbase distribution])
   fi

   PHP_ADD_LIBRARY(pthread,,OCEANBASE_SHARED_LIBADD)
   PHP_ADD_LIBRARY(numa,,OCEANBASE_SHARED_LIBADD)
   PHP_ADD_LIBRARY(rt,,OCEANBASE_SHARED_LIBADD)
   AC_DEFINE(HAVE_OCEANBASELIB,1,[ ])
   PHP_SUBST(OCEANBASE_SHARED_LIBADD)

   PHP_NEW_EXTENSION(oceanbase, php_oceanbase.cpp, $ext_shared)
fi
