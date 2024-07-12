def compute_cooc(mtx):
    return (mtx.T * mtx).todense()