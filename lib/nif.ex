defmodule HurstFdNif do

  @on_load :load_nif
  def load_nif do
    nif_file = :filename.join(:code.priv_dir(:hurst_fd), "hurst_fd")
    :erlang.load_nif(nif_file, 0)
  end

  def compute_rs(_log_returns, _num_samples, _window_sizes) do
    :erlang.nif_error(:not_loaded)
  end

end
