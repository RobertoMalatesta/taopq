// Copyright (c) 2016-2020 Daniel Frey and Dr. Colin Hirsch
// Please see LICENSE for license or visit https://github.com/taocpp/taopq/

#ifndef TAO_PQ_CONNECTION_POOL_HPP
#define TAO_PQ_CONNECTION_POOL_HPP

#include <memory>
#include <string>
#include <utility>

#include <tao/pq/internal/pool.hpp>

#include <tao/pq/connection.hpp>
#include <tao/pq/parameter_traits.hpp>
#include <tao/pq/result.hpp>

namespace tao::pq
{
   template< template< typename... > class DefaultTraits = parameter_text_traits >
   class connection_pool
      : public internal::pool< connection< DefaultTraits > >
   {
   private:
      const std::string m_connection_info;

      [[nodiscard]] auto v_create() const -> std::unique_ptr< connection< DefaultTraits > > override
      {
         return std::make_unique< pq::connection< DefaultTraits > >( m_connection_info );
      }

      [[nodiscard]] auto v_is_valid( connection< DefaultTraits >& c ) const noexcept -> bool override
      {
         return c.is_open();
      }

   public:
      explicit connection_pool( const std::string& connection_info ) noexcept  // NOLINT(modernize-pass-by-value)
         : m_connection_info( connection_info )
      {}

      [[nodiscard]] static auto create( const std::string& connection_info ) -> std::shared_ptr< connection_pool >
      {
         return std::make_shared< connection_pool >( connection_info );
      }

      [[nodiscard]] auto connection()
      {
         return this->get();
      }

      template< template< typename... > class Traits = DefaultTraits, typename... Ts >
      auto execute( Ts&&... ts )
      {
         return this->connection()->direct()->template execute< Traits >( std::forward< Ts >( ts )... );
      }
   };

}  // namespace tao::pq

#endif
