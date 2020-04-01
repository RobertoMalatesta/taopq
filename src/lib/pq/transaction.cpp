// Copyright (c) 2016-2020 Daniel Frey and Dr. Colin Hirsch
// Please see LICENSE for license or visit https://github.com/taocpp/taopq/

#include <tao/pq/transaction.hpp>

#include <stdexcept>

#include <tao/pq/connection.hpp>
#include <tao/pq/internal/printf.hpp>

namespace tao::pq
{
   basic_transaction::basic_transaction( const std::shared_ptr< basic_connection >& connection )  // NOLINT(modernize-pass-by-value)
      : m_connection( connection )
   {}

   basic_transaction::~basic_transaction() = default;

   auto basic_transaction::current_transaction() const noexcept -> basic_transaction*&
   {
      return m_connection->m_current_transaction;
   }

   void basic_transaction::check_current_transaction() const
   {
      if( !m_connection || this != current_transaction() ) {
         throw std::logic_error( "transaction order error" );
      }
   }

   auto basic_transaction::execute_params( const char* statement,
                                           const int n_params,
                                           const Oid types[],
                                           const char* const values[],
                                           const int lengths[],
                                           const int formats[] ) -> result
   {
      check_current_transaction();
      return m_connection->execute_params( statement, n_params, types, values, lengths, formats );
   }

   auto basic_transaction::underlying_raw_ptr() const noexcept -> PGconn*
   {
      return m_connection->underlying_raw_ptr();
   }

   void basic_transaction::commit()
   {
      check_current_transaction();
      try {
         v_commit();
      }
      // LCOV_EXCL_START
      catch( ... ) {
         v_reset();
         throw;
      }
      // LCOV_EXCL_STOP
      v_reset();
   }

   void basic_transaction::rollback()
   {
      check_current_transaction();
      try {
         v_rollback();
      }
      // LCOV_EXCL_START
      catch( ... ) {
         v_reset();
         throw;
      }
      // LCOV_EXCL_STOP
      v_reset();
   }

}  // namespace tao::pq
