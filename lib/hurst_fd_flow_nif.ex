defmodule HurstFdFlowNif do

  require Explorer.DataFrame, as: DF
  alias Explorer.Series, as: S

  defp hurst_fd(series, window, min_window \\ 10, max_window \\ 100, num_windows \\ 20, num_samples \\ 100) do

    window_sizes = Nx.linspace(min_window, max_window, n: num_windows, type: {:s, 32})
    log_window_sizes = Nx.log(window_sizes) |> Nx.new_axis(-1)
    window_sizes = window_sizes |> Nx.to_list()
    results =
      Enum.reduce(0..(S.count(series) - 1 - window), [], fn index, slices ->
        slice = series |> S.slice(index, window)
        [{index, slice} | slices]
      end)
      |> Flow.from_enumerable([min_demand: 25, max_demand: 50])
      |> Flow.reduce(fn -> [] end, fn {index, slice}, acc ->
          log_returns = slice
            |> S.log
            |> then(&S.subtract(&1, S.shift(&1, -1)))
            |> S.fill_missing(:forward)
          {exponent, dimension} = window_sizes_loop(log_returns, window_sizes, log_window_sizes, num_samples)
          [%{exponent: exponent, dimension: dimension, index: index + window - 1} | acc]
        end)
      |> Enum.to_list()

    DF.new(results)
  end

  defp window_sizes_loop(slice, window_sizes, log_window_sizes, num_samples) do

    r_s = HurstFdNif.compute_rs(slice |> S.to_iovec, num_samples, window_sizes)
    log_r_s = Nx.from_binary(r_s, :f64)|> Nx.log
    model = Scholar.Linear.PolynomialRegression.fit(log_window_sizes, log_r_s, degree: 1)

    hurst_exponent = model.coefficients[0] |> Nx.to_number
    fractal_dimension = 2 - hurst_exponent
    {hurst_exponent, fractal_dimension}
  end

  def run do
    df = DF.from_csv!("/home/oz/elixir/hurst_fd/lib/ASML.csv")
    result_df = hurst_fd(df["Close"], 120)
    # result_df = hurst_fd(df["Close"] |> S.head(200), 120)
    df
      |> DF.mutate_with(&[index: S.row_index(&1[:Close]) |> S.cast({:s, 64})])
      |> DF.join(result_df, [on: ["index"], how: :left])
      |> DF.relocate("index", before: 0)
  end


end
