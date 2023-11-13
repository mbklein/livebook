defmodule Livebook.FileSystem.S3.Credentials do
  use GenServer

  alias Livebook.FileSystem.S3

  @default_config [
    ec2_base: "http://169.254.169.254/latest/meta-data/iam/security-credentials",
    ecs_base: "http://169.254.170.2",
    ecs_var: "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    access_key_id_var: "AWS_ACCESS_KEY_ID",
    secret_access_key_var: "AWS_SECRET_ACCESS_KEY"
  ]

  @spec start_link(keyword) :: {:ok, pid()}
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @impl true
  def init(config \\ []) do
    config = Keyword.merge(@default_config, config) |> Enum.into(%{})
    state = %{config: config, method: :unknown, credentials: %{}}
    {:ok, state}
  end

  @doc """
  Ensures that the given S3 FileSystem struct has credentials, and populates them
  from EC2 instance metadata or ECS container metadata if possible
  """
  @spec ensure_credentials(S3.t()) :: S3.t()
  def ensure_credentials(%S3{access_key_id: nil, secret_access_key: nil} = file_system) do
    GenServer.call(Livebook.S3Credentials, {:ensure_credentials, file_system})
  end

  def ensure_credentials(file_system), do: file_system

  @impl true
  def handle_call({:ensure_credentials, file_system}, _, state) do
    case validate_credentials(state) do
      {:ok, state} ->
        {:reply, add_credentials(file_system, state), state}

      {_, state} ->
        case get_credentials(state) do
          {:ok, new_state} -> {:reply, add_credentials(file_system, new_state), new_state}
          {:error, error} -> {:stop, error, state}
        end
    end
  end

  # The following `handle_call` and `handle_info` implementations exist only to allow
  # tests to reconfigure the GenServer and pre-set cached credentials

  def handle_call(:config, _, state), do: {:reply, state[:config], state}

  @impl true
  def handle_info({:config, config}, state), do: {:noreply, Map.put(state, :config, config)}

  def handle_info({:credentials, credentials}, state),
    do: {:noreply, Map.put(state, :credentials, credentials)}

  def handle_info(:reset, state),
    do: {:noreply, Map.merge(state, %{credentials: %{}, method: :unknown})}

  defp add_credentials(file_system, %{credentials: credentials}) do
    %S3{
      file_system
      | access_key_id: credentials[:access_key_id],
        secret_access_key: credentials[:secret_access_key]
    }
  end

  defp validate_credentials(%{credentials: credentials} = state) do
    case credentials do
      %{expiration: :none} ->
        {:ok, state}

      %{expiration: expiration} ->
        if NaiveDateTime.diff(expiration, NaiveDateTime.utc_now()) > 5,
          do: {:ok, state},
          else: {:expired, state}

      _ ->
        {:missing, state}
    end
  end

  defp get_credentials(%{config: config, method: :unknown} = state) do
    state
    |> Map.put(:method, infer_method(config))
    |> get_credentials()
  end

  defp get_credentials(%{config: config, method: :environment} = state) do
    {:ok,
     state
     |> Map.put(:credentials, %{
       access_key_id: System.get_env(config[:access_key_id_var]),
       secret_access_key: System.get_env(config[:secret_access_key_var]),
       expiration: :none
     })}
  end

  defp get_credentials(%{config: config, method: :ec2} = state) do
    case Livebook.Utils.HTTP.request(:get, config[:ec2_base]) do
      {:ok, 200, _, body} ->
        [role | _] = String.split(body, "\n")
        retrieve_credentials(config[:ec2_base] <> "/" <> role, state)

      other ->
        {:error, other}
    end
  end

  defp get_credentials(%{config: config, method: :ecs} = state) do
    with url <- config[:ecs_base] <> System.get_env(config[:ecs_var]) do
      retrieve_credentials(url, state)
    end
  end

  defp retrieve_credentials(url, state) do
    Livebook.Utils.HTTP.request(:get, url)
    |> update_credentials(state)
  end

  defp update_credentials({:ok, 200, _, body}, state) do
    case Jason.decode(body) do
      {:ok,
       %{
         "AccessKeyId" => access_key_id,
         "SecretAccessKey" => secret_access_key,
         "Expiration" => expiration
       }} ->
        {:ok,
         state
         |> Map.put(:credentials, %{
           access_key_id: access_key_id,
           secret_access_key: secret_access_key,
           expiration: NaiveDateTime.from_iso8601!(expiration)
         })}

      {:ok, other} ->
        {:error, "Expected valid credential map, got: #{other}"}

      {:error, error} ->
        {:error, error}
    end
  end

  defp update_credentials(response, _), do: {:error, response}

  defp infer_method(config) do
    cond do
      environment?(config) -> :environment
      ecs?(config) -> :ecs
      ec2?(config) -> :ec2
      true -> :none
    end
  end

  defp environment?(%{
         access_key_id_var: access_key_id_var,
         secret_access_key_var: secret_access_key_var
       }) do
    !!(System.get_env(access_key_id_var) && System.get_env(secret_access_key_var))
  end

  defp ecs?(%{ecs_var: ecs_var}) do
    !!System.get_env(ecs_var)
  end

  defp ec2?(%{ec2_base: ec2_base}) do
    case Livebook.Utils.HTTP.request(:get, ec2_base) do
      {:ok, 200, _, ""} -> false
      {:ok, 200, _, _} -> true
      _ -> false
    end
  end
end
