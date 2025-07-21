


use thiserror::Error;




#[derive(Debug, Clone, Error, PartialEq)]
pub enum RenderBuildError {
    // #[error("No graph for anchor node.")]
    // MissingAnchorNodeGraphTraversal,

    #[error("Error while building Select items from logical plan")]
    SelectItemsBuilder

    
}

