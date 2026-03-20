"""Resolves MongoDB references to decomposed documents."""

class ReferenceResolver:
    """Handles resolution of decomposed document references."""
    
    def __init__(self, mongo_handler):
        self.mongo = mongo_handler
    
    def is_reference(self, value):
        """Check if a value is a decomposed reference."""
        if not isinstance(value, str):
            return False
        return value.startswith("REF::MONGO::")
    
    def resolve_reference(self, reference_string):
        """
        Resolve a reference to a decomposed MongoDB document.
        
        Reference format: REF::MONGO::collection_name::parent_uuid
        Returns: List of data payloads or empty list if not found
        """
        if not self.is_reference(reference_string):
            return reference_string
        
        try:
            parts = reference_string.split("::")
            if len(parts) != 4:
                return reference_string
            
            coll_name = parts[2]
            parent_uuid = parts[3]
            
            if self.mongo.db is None:
                return []
            
            # Fetch all documents from decomposed collection with this parent
            docs = list(self.mongo.db[coll_name].find({"parent_uuid": parent_uuid}))
            
            # Return list of data payloads
            result = [doc.get("data") for doc in docs if "data" in doc]
            return result if result else []
        
        except Exception as e:
            print(f"[ReferenceResolver] Failed to resolve reference {reference_string}: {e}")
            return []
    
    def resolve_all_references(self, doc):
        """
        Recursively resolve all references in a document.
        
        Handles nested dictionaries and lists.
        """
        if isinstance(doc, dict):
            resolved = {}
            for k, v in doc.items():
                if self.is_reference(v):
                    # Resolve reference
                    resolved[k] = self.resolve_reference(v)
                elif isinstance(v, dict):
                    resolved[k] = self.resolve_all_references(v)
                elif isinstance(v, list):
                    resolved[k] = [
                        self.resolve_all_references(item) if isinstance(item, dict) else item
                        for item in v
                    ]
                else:
                    resolved[k] = v
            return resolved
        elif isinstance(doc, list):
            return [
                self.resolve_all_references(item) if isinstance(item, dict) else item
                for item in doc
            ]
        else:
            return doc
