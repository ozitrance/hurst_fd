defmodule HurstFd do

  require Explorer.DataFrame, as: DF
  alias Explorer.Series, as: S



# NOT COMPLETE / ABANDONED CODE - RESULT (IF THERE'S ONE) IS WRONG


  defp hurst_fd(series, window, min_window \\ 10, max_window \\ 100, num_windows \\ 20, num_samples \\ 100) do

    log_returns = series
      |> S.log
      |> then(&S.subtract(&1, S.shift(&1, -1)))

    window_sizes = Nx.linspace(min_window, max_window, n: num_windows, type: {:s, 32})

    {exponents, dimensions} =
      Enum.reduce(window..(S.count(log_returns) - 1 - window), {[], []}, fn index, {exponents, dimensions} ->
        slice = log_returns |> S.slice(index, window)
        {exponent, dimension} = window_sizes_loop(slice, window_sizes, num_samples)
        IO.inspect(exponent)
        IO.inspect(dimension)
        {[exponent | exponents], [dimension | dimensions]}
      end)

    IO.inspect({exponents, dimensions})
  end

  defp window_sizes_loop(slice, window_sizes, num_samples) do
    IO.inspect("before r_s loop")
    r_s = Enum.reduce(window_sizes |> Nx.to_list, [], fn w, r_s ->
      {r, s} = num_samples_range_loop(slice, w, num_samples)
      new_r_s = (S.from_list(r) |> S.mean) / (S.from_list(s) |> S.mean)
      [new_r_s | r_s]

    end)

    log_window_sizes = Nx.log(window_sizes) |> Nx.new_axis(-1)
    log_r_s = Enum.reverse(r_s) |> Nx.tensor |> Nx.log
    IO.inspect(log_window_sizes)
    IO.inspect(log_r_s)

    model = Scholar.Linear.PolynomialRegression.fit(log_window_sizes, log_r_s, degree: 1)

    hurst_exponent = model.coefficients[0] |> Nx.to_number
    fractal_dimension = 2 - hurst_exponent
    IO.inspect(hurst_exponent, label: "hurst_exponent")
    IO.inspect(fractal_dimension, label: "fractal_dimension")
    # System.halt
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
    hurst_fd(df["Close"], 120)

  end






end
