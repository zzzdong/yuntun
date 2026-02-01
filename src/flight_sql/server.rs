//! Arrow Flight server implementation

use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use arrow_flight::flight_service_server::FlightServiceServer;

use crate::flight_sql::service::YuntunFlightService;
use crate::query::query_service::QueryService;
use crate::meta::service::MetaService;
use crate::store::store_manager::StoreManager;

/// Flight server
#[derive(Debug, Clone)]
pub struct YuntunFlightServer {
    service: YuntunFlightService,
    addr: SocketAddr,
}

impl YuntunFlightServer {
    /// Create a new Flight server
    pub fn new(
        query_service: Arc<QueryService>,
        meta_service: Arc<MetaService>,
        store_manager: Arc<StoreManager>,
        addr: SocketAddr
    ) -> Self {
        let service = YuntunFlightService::new(
            query_service,
            meta_service,
            store_manager
        );
        
        Self {
            service,
            addr,
        }
    }
    
    /// Start the Flight server
    pub async fn start(&self) -> Result<(), anyhow::Error> {
        println!("Starting Flight server on {}", self.addr);
        
        let server = Server::builder()
            .add_service(FlightServiceServer::new(self.service.clone()));
        
        server.serve(self.addr).await?;
        
        Ok(())
    }
}