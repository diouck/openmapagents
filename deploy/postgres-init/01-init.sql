-- Active l'extension pgvector (RAG / embeddings).
-- Exécuté automatiquement au tout premier démarrage du conteneur postgres
-- (quand le volume de données est vide).
CREATE EXTENSION IF NOT EXISTS vector;
