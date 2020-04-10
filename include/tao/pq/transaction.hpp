// Copyright (c) 2016-2020 Daniel Frey and Dr. Colin Hirsch
// Please see LICENSE for license or visit https://github.com/taocpp/taopq/

#ifndef TAO_PQ_TRANSACTION_HPP
#define TAO_PQ_TRANSACTION_HPP

#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include <tao/pq/internal/dependent_false.hpp>
#include <tao/pq/internal/gen.hpp>
#include <tao/pq/internal/printf.hpp>
#include <tao/pq/parameter_traits.hpp>
#include <tao/pq/result.hpp>

namespace tao::pq
{
   class basic_connection;
   class table_writer;

   class basic_transaction
      : public std::enable_shared_from_this< basic_transaction >
   {
   public:
      enum class isolation_level
      {
         default_isolation_level,
         serializable,
         repeatable_read,
         read_committed,
         read_uncommitted
      };
      friend class table_writer;

   protected:
      std::shared_ptr< basic_connection > m_connection;

      explicit basic_transaction( const std::shared_ptr< basic_connection >& connection );
      virtual ~basic_transaction() = 0;

   public:
      basic_transaction( const basic_transaction& ) = delete;
      basic_transaction( basic_transaction&& ) = delete;
      void operator=( const basic_transaction& ) = delete;
      void operator=( basic_transaction&& ) = delete;

   protected:
      [[nodiscard]] virtual auto v_is_direct() const noexcept -> bool = 0;

      virtual void v_commit() = 0;
      virtual void v_rollback() = 0;

      virtual void v_reset() noexcept = 0;

      [[nodiscard]] auto current_transaction() const noexcept -> basic_transaction*&;
      void check_current_transaction() const;

      [[nodiscard]] auto execute_params( const char* statement,
                                         const int n_params,
                                         const Oid types[],
                                         const char* const values[],
                                         const int lengths[],
                                         const int formats[] ) -> result;

      auto underlying_raw_ptr() const noexcept -> PGconn*;

   public:
      void commit();
      void rollback();
   };

   template< template< typename... > class DefaultTraits >
   class transaction
      : public basic_transaction
   {
   private:
      template< std::size_t... Os, std::size_t... Is, typename... Ts >
      [[nodiscard]] auto execute_indexed( const char* statement,
                                          std::index_sequence< Os... > /*unused*/,
                                          std::index_sequence< Is... > /*unused*/,
                                          const std::tuple< Ts... >& tuple )
      {
         const Oid types[] = { std::get< Os >( tuple ).template type< Is >()... };
         const char* const values[] = { std::get< Os >( tuple ).template value< Is >()... };
         const int lengths[] = { std::get< Os >( tuple ).template length< Is >()... };
         const int formats[] = { std::get< Os >( tuple ).template format< Is >()... };
         return execute_params( statement, sizeof...( Os ), types, values, lengths, formats );
      }

      template< typename... Ts >
      [[nodiscard]] auto execute_traits( const char* statement, const Ts&... ts )
      {
         using gen = internal::gen< Ts::columns... >;
         return execute_indexed( statement, typename gen::outer_sequence(), typename gen::inner_sequence(), std::tie( ts... ) );
      }

      template< template< typename... > class Traits, typename A >
      auto to_traits( A&& a ) const
      {
         using T = Traits< std::decay_t< A > >;
         if constexpr( std::is_constructible_v< T, decltype( std::forward< A >( a ) ) > ) {
            return T( std::forward< A >( a ) );
         }
         else if constexpr( std::is_constructible_v< T, PGconn*, decltype( std::forward< A >( a ) ) > ) {
            return T( this->underlying_raw_ptr(), std::forward< A >( a ) );
         }
         else {
            static_assert( internal::dependent_false< T >, "no valid conversion from A to Traits" );
         }
      }

   public:
      explicit transaction( const std::shared_ptr< basic_connection >& connection )
         : basic_transaction( connection )
      {}

      ~transaction() override = default;

      transaction( const transaction& ) = delete;
      transaction( transaction&& ) = delete;
      void operator=( const transaction& ) = delete;
      void operator=( transaction&& ) = delete;

      template< template< typename... > class Traits = DefaultTraits >
      [[nodiscard]] auto subtransaction() -> std::shared_ptr< transaction< Traits > >;

      template< template< typename... > class Traits = DefaultTraits, typename... As >
      auto execute( const char* statement, As&&... as )
      {
         return execute_traits( statement, to_traits< Traits >( std::forward< As >( as ) )... );
      }

      // short-cut for no-arguments invocations
      template< template< typename... > class Traits = DefaultTraits >
      auto execute( const char* statement )
      {
         return execute_params( statement, 0, nullptr, nullptr, nullptr, nullptr );
      }

      template< template< typename... > class Traits = DefaultTraits, typename... As >
      auto execute( const std::string& statement, As&&... as )
      {
         return execute< Traits >( statement.c_str(), std::forward< As >( as )... );
      }
   };

   template< template< typename... > class Traits >
   class subtransaction_base
      : public transaction< Traits >
   {
   private:
      const std::shared_ptr< basic_transaction > m_previous;

   protected:
      explicit subtransaction_base( const std::shared_ptr< basic_connection >& connection )
         : transaction< Traits >( connection ),
           m_previous( this->current_transaction()->shared_from_this() )
      {
         this->current_transaction() = this;
      }

      ~subtransaction_base() override
      {
         if( this->m_connection ) {
            this->current_transaction() = m_previous.get();  // LCOV_EXCL_LINE
         }
      }

      [[nodiscard]] auto v_is_direct() const noexcept -> bool override
      {
         return false;
      }

      void v_reset() noexcept override
      {
         this->current_transaction() = m_previous.get();
         this->m_connection.reset();
      }

   public:
      subtransaction_base( const subtransaction_base& ) = delete;
      subtransaction_base( subtransaction_base&& ) = delete;
      void operator=( const subtransaction_base& ) = delete;
      void operator=( subtransaction_base&& ) = delete;
   };

   template< template< typename... > class Traits >
   class top_level_subtransaction final
      : public subtransaction_base< Traits >
   {
   public:
      explicit top_level_subtransaction( const std::shared_ptr< basic_connection >& connection )
         : subtransaction_base< Traits >( connection )
      {
         this->execute( "START TRANSACTION" );
      }

      ~top_level_subtransaction() override
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

      top_level_subtransaction( const top_level_subtransaction& ) = delete;
      top_level_subtransaction( top_level_subtransaction&& ) = delete;
      void operator=( const top_level_subtransaction& ) = delete;
      void operator=( top_level_subtransaction&& ) = delete;

   private:
      void v_commit() override
      {
         this->execute( "COMMIT TRANSACTION" );
      }

      void v_rollback() override
      {
         this->execute( "ROLLBACK TRANSACTION" );
      }
   };

   template< template< typename... > class Traits >
   class nested_subtransaction final
      : public subtransaction_base< Traits >
   {
   public:
      explicit nested_subtransaction( const std::shared_ptr< basic_connection >& connection )
         : subtransaction_base< Traits >( connection )
      {
         this->execute( internal::printf( "SAVEPOINT \"TAOPQ_%p\"", static_cast< void* >( this ) ) );
      }

      ~nested_subtransaction() override
      {
         if( this->m_connection && this->m_connection->is_open() ) {
            try {
               this->rollback();
            }
            // LCOV_EXCL_START
            catch( const std::exception& ) {
               // TODO: Add more information about exception when available
               // TAO_LOG( WARNING, "unable to rollback transaction, swallowing exception: " + std::string( e.what() ) );
            }
            catch( ... ) {
               // TAO_LOG( WARNING, "unable to rollback transaction, swallowing unknown exception" );
            }
            // LCOV_EXCL_STOP
         }
      }

      nested_subtransaction( const nested_subtransaction& ) = delete;
      nested_subtransaction( nested_subtransaction&& ) = delete;
      void operator=( const nested_subtransaction& ) = delete;
      void operator=( nested_subtransaction&& ) = delete;

   private:
      void v_commit() override
      {
         this->execute( internal::printf( "RELEASE SAVEPOINT \"TAOPQ_%p\"", static_cast< void* >( this ) ) );
      }

      void v_rollback() override
      {
         this->execute( internal::printf( "ROLLBACK TO \"TAOPQ_%p\"", static_cast< void* >( this ) ) );
      }
   };

   template< template< typename... > class DefaultTraits >
   template< template< typename... > class Traits >
   auto transaction< DefaultTraits >::subtransaction() -> std::shared_ptr< transaction< Traits > >
   {
      check_current_transaction();
      if( v_is_direct() ) {
         return std::make_shared< top_level_subtransaction< Traits > >( m_connection );
      }
      return std::make_shared< nested_subtransaction< Traits > >( m_connection );
   }

}  // namespace tao::pq

#endif
