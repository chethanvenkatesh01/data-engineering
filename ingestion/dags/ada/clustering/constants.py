from .helpers import meanshift,faiss_helper
MIN_SAMPLES_CLUSTERING = 11


mapper = {
    'meanshift': meanshift,
    'faiss':faiss_helper
}
