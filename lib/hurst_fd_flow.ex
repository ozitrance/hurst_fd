defmodule HurstFdFlow do

  require Explorer.DataFrame, as: DF
  alias Explorer.Series, as: S

  defp hurst_fd(series, window, min_window \\ 10, max_window \\ 100, num_windows \\ 20, num_samples \\ 100) do

    window_sizes = Nx.linspace(min_window, max_window, n: num_windows, type: {:s, 32})

    Enum.reduce(0..(S.count(series) - 1 - window), [], fn index, slices ->
        slice = series |> S.slice(index, window)
        [{index, slice} | slices]
    end)
      |> Flow.from_enumerable([min_demand: 25, max_demand: 50])
      |> Flow.reduce(fn -> [] end, fn {index, slice}, acc ->
          log_returns = slice
            |> S.log
            |> then(&S.subtract(&1, S.shift(&1, 1)))
          IO.inspect(index)
          {exponent, dimension} = window_sizes_loop(log_returns, window_sizes, num_samples)
          [%{exponent: exponent, dimension: dimension, index: index + window - 1} | acc]
        end)
      |> Enum.to_list()
      |> DF.new

  end

  defp window_sizes_loop(slice, window_sizes, num_samples) do
    r_s =
      Enum.reduce(window_sizes |> Nx.to_list, [], fn w, r_s ->
        {r, s} = num_samples_range_loop(slice, w, num_samples)
        new_r_s = (S.from_list(r) |> S.mean) / (S.from_list(s) |> S.mean)
        [new_r_s | r_s]

      end)

    log_window_sizes = Nx.log(window_sizes) |> Nx.new_axis(-1)
    log_r_s = Enum.reverse(r_s) |> Nx.tensor |> Nx.log

    model = Scholar.Linear.PolynomialRegression.fit(log_window_sizes, log_r_s, degree: 1)

    hurst_exponent = model.coefficients[0] |> Nx.to_number
    fractal_dimension = 2 - hurst_exponent
    {hurst_exponent, fractal_dimension}
  end


  defp num_samples_range_loop(slice, w, num_samples) do
    random_key = Nx.Random.key(System.os_time)

    Enum.reduce(0..(num_samples - 1), {[], [], random_key}, fn _idx, {r, s, random_key} ->
      {start, random_key} = Nx.Random.randint(random_key, 0, S.count(slice) - w) |> then(fn {s,r} -> {Nx.to_number(s), r} end)
      seq = slice |> S.slice(start, w)
      {[S.max(seq) - S.min(seq) | r], [S.standard_deviation(seq) | s], random_key}

    end)
      |> then(fn {r, s, _random_key} -> {Enum.reverse(r), Enum.reverse(s)} end)

  end

  def run do
    df = DF.from_csv!("ASML.csv")
    result_df = hurst_fd(df["Close"], 120)
    df
      |> DF.mutate_with(&[index: S.row_index(&1[:Close]) |> S.cast({:s, 64})])
      |> DF.join(result_df, [on: ["index"], how: :left])
      |> DF.relocate("index", before: 0)
  end


end
