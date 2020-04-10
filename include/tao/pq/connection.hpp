// Copyright (c) 2016-2020 Daniel Frey and Dr. Colin Hirsch
// Please see LICENSE for license or visit https://github.com/taocpp/taopq/

#ifndef TAO_PQ_CONNECTION_HPP
#define TAO_PQ_CONNECTION_HPP

#include <memory>
#include <set>
#include <string>
#include <utility>

#include <libpq-fe.h>

#include <tao/pq/internal/unreachable.hpp>
#include <tao/pq/result.hpp>
#include <tao/pq/transaction.hpp>

namespace tao::pq
{
   template< template< typename... > class DefaultTraits >
   class connection_pool;

   class table_writer;

   namespace internal
   {
      struct deleter final
      {
         void operator()( PGconn* p ) const noexcept
         {
            PQfinish( p );
         }
      };

   }  // namespace internal

   class basic_connection
      : public std::enable_shared_from_this< basic_connection >
   {
   private:
      // friend class connection_pool;
      friend class basic_transaction;
      friend class table_writer;

      const std::unique_ptr< PGconn, internal::deleter > m_pgconn;
      basic_transaction* m_current_transaction;
      std::set< std::string, std::less<> > m_prepared_statements;

      [[nodiscard]] auto error_message() const -> std::string;
      static void check_prepared_name( const std::string& name );
      [[nodiscard]] auto is_prepared( const char* name ) const noexcept -> bool;

      [[nodiscard]] auto execute_params( const char* statement,
                                         const int n_params,
                                         const Oid types[],
                                         const char* const values[],
                                         const int lengths[],
                                         const int formats[] ) -> result;

   public:
      explicit basic_connection( const std::string& connection_info );

      basic_connection( const basic_connection& ) = delete;
      basic_connection( basic_connection&& ) = delete;
      void operator=( const basic_connection& ) = delete;
      void operator=( basic_connection&& ) = delete;

      ~basic_connection() = default;

      [[nodiscard]] auto is_open() const noexcept -> bool;

      void prepare( const std::string& name, const std::string& statement );
      void deallocate( const std::string& name );

      [[nodiscard]] auto underlying_raw_ptr() noexcept -> PGconn*
      {
         return m_pgconn.get();
      }

      [[nodiscard]] auto underlying_raw_ptr() const noexcept -> const PGconn*
      {
         return m_pgconn.get();
      }
   };

   template< template< typename... > class Traits >
   class transaction_base
      : public transaction< Traits >
   {
   protected:
      explicit transaction_base( const std::shared_ptr< basic_connection >& connection )
         : transaction< Traits >( connection )
      {
         if( this->current_transaction() != nullptr ) {
            throw std::logic_error( "transaction order error" );
         }
         this->current_transaction() = this;
      }

      ~transaction_base() override
      {
         if( this->m_connection ) {
            this->current_transaction() = nullptr;
         }
      }

      void v_reset() noexcept override
      {
         this->current_transaction() = nullptr;
         this->m_connection.reset();
      }

   public:
      transaction_base( const transaction_base& ) = delete;
      transaction_base( transaction_base&& ) = delete;
      void operator=( const transaction_base& ) = delete;
      void operator=( transaction_base&& ) = delete;
   };

   template< template< typename... > class Traits >
   class autocommit_transaction final
      : public transaction_base< Traits >
   {
   public:
      explicit autocommit_transaction( const std::shared_ptr< basic_connection >& connection )
         : transaction_base< Traits >( connection )
      {}

   private:
      [[nodiscard]] auto v_is_direct() const noexcept -> bool override
      {
         return true;
      }

      void v_commit() override
      {}

      void v_rollback() override
      {}
   };

   template< template< typename... > class Traits >
   class top_level_transaction final
      : public transaction_base< Traits >
   {
   private:
      [[nodiscard]] static auto isolation_level_to_statement( const basic_transaction::isolation_level il ) -> const char*
      {
         switch( il ) {
            case basic_transaction::isolation_level::default_isolation_level:
               return "START TRANSACTION";
            case basic_transaction::isolation_level::serializable:
               return "START TRANSACTION ISOLATION LEVEL SERIALIZABLE";
            case basic_transaction::isolation_level::repeatable_read:
               return "START TRANSACTION ISOLATION LEVEL REPEATABLE READ";
            case basic_transaction::isolation_level::read_committed:
               return "START TRANSACTION ISOLATION LEVEL READ COMMITTED";
            case basic_transaction::isolation_level::read_uncommitted:
               return "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED";
         }
         TAO_PQ_UNREACHABLE;
      }

   public:
      explicit top_level_transaction( const basic_transaction::isolation_level il, const std::shared_ptr< basic_connection >& connection )
         : transaction_base< Traits >( connection )
      {
         this->execute( isolation_level_to_statement( il ) );
      }

      ~top_level_transaction() override
      {
         if( this->m_connection && this->m_connection->is_open() ) {
            try {
               this->rollback();
            }
            // LCOV_EXCL_START
            catch( const std::exception& ) {
               // TAO_LOG( WARNING, "unable to rollback transaction, swallowing exception: " + std::string( e.what() ) );
            }
            catch( ... ) {
               // TAO_LOG( WARNING, "unable to rollback transaction, swallowing unknown exception" );
            }
            // LCOV_EXCL_STOP
         }
      }

      top_level_transaction( const top_level_transaction& ) = delete;
      top_level_transaction( top_level_transaction&& ) = delete;
      void operator=( const top_level_transaction& ) = delete;
      void operator=( top_level_transaction&& ) = delete;

   private:
      [[nodiscard]] auto v_is_direct() const noexcept -> bool override
      {
         return false;
      }

      void v_commit() override
      {
         this->execute( "COMMIT TRANSACTION" );
      }

      void v_rollback() override
      {
         this->execute( "ROLLBACK TRANSACTION" );
      }
   };

   template< template< typename... > class DefaultTraits = parameter_text_traits >
   class connection final
      : public basic_connection
   {
   public:
      explicit connection( const std::string& connection_info )
         : basic_connection( connection_info )
      {}

      connection( const connection& ) = delete;
      connection( connection&& ) = delete;
      void operator=( const connection& ) = delete;
      void operator=( connection&& ) = delete;

      ~connection() = default;

      [[nodiscard]] static auto create( const std::string& connection_info )
      {
         return std::make_shared< connection >( connection_info );
      }

      template< template< typename... > class Traits = DefaultTraits >
      [[nodiscard]] auto direct() -> std::shared_ptr< transaction< Traits > >
      {
         return std::make_shared< autocommit_transaction< Traits > >( shared_from_this() );
      }

      template< template< typename... > class Traits = DefaultTraits >
      [[nodiscard]] auto transaction( const basic_transaction::isolation_level il = basic_transaction::isolation_level::default_isolation_level ) -> std::shared_ptr< transaction< Traits > >
      {
         return std::make_shared< top_level_transaction< Traits > >( il, shared_from_this() );
      }

      template< template< typename... > class Traits = DefaultTraits, typename... Ts >
      auto execute( Ts&&... ts )
      {
         return direct()->template execute< Traits >( std::forward< Ts >( ts )... );
      }
   };

}  // namespace tao::pq

#endif
