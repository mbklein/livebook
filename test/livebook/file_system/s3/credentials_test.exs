defmodule Livebook.FileSystem.S3.CredentialsTest do
  use ExUnit.Case, async: false
  alias Livebook.FileSystem.S3

  describe "ensure_credentials/1" do
    setup do
      bypass = Bypass.open()

      old_config = GenServer.call(Livebook.S3Credentials, :config)

      send(
        Livebook.S3Credentials,
        {:config,
         %{
           ec2_base:
             "http://localhost:#{bypass.port}/ec2/latest/meta-data/iam/security-credentials",
           ecs_base: "http://localhost:#{bypass.port}/ecs",
           ecs_var: "TEST_AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
           access_key_id_var: "TEST_AWS_ACCESS_KEY_ID",
           secret_access_key_var: "TEST_AWS_SECRET_ACCESS_KEY"
         }}
      )

      on_exit(fn ->
        send(Livebook.S3Credentials, {:config, old_config})
        send(Livebook.S3Credentials, :reset)
      end)

      {:ok, %{bypass: bypass}}
    end

    test "uses existing credentials" do
      id = "preconfigured_id"
      secret = "preconfigured_secret"

      assert %S3{access_key_id: ^id, secret_access_key: ^secret} =
               %S3{access_key_id: id, secret_access_key: secret}
               |> S3.Credentials.ensure_credentials()
    end

    test "retrieves environment credentials" do
      assert %S3{access_key_id: "environment_key_id", secret_access_key: "environment_secret_key"} =
               with_env(
                 [
                   TEST_AWS_ACCESS_KEY_ID: "environment_key_id",
                   TEST_AWS_SECRET_ACCESS_KEY: "environment_secret_key"
                 ],
                 fn -> %S3{} |> S3.Credentials.ensure_credentials() end
               )
    end

    test "retrieves EC2 credentials", %{bypass: bypass} do
      Bypass.expect(
        bypass,
        "GET",
        "/ec2/latest/meta-data/iam/security-credentials",
        fn conn ->
          conn
          |> Plug.Conn.put_resp_content_type("text/plain")
          |> Plug.Conn.resp(200, "ec2-instance-role\n")
        end
      )

      Bypass.expect(
        bypass,
        "GET",
        "/ec2/latest/meta-data/iam/security-credentials/ec2-instance-role",
        fn conn ->
          conn
          |> Plug.Conn.put_resp_content_type("application/json")
          |> Plug.Conn.resp(200, credentials("ec2"))
        end
      )

      assert %S3{access_key_id: "ec2_access_key_id", secret_access_key: "ec2_secret_access_key"} =
               %S3{} |> S3.Credentials.ensure_credentials()
    end

    test "retrieves ECS credentials", %{bypass: bypass} do
      Bypass.expect(
        bypass,
        "GET",
        "/ecs/ecs_credential_path",
        fn conn ->
          conn
          |> Plug.Conn.put_resp_content_type("application/json")
          |> Plug.Conn.resp(200, credentials("ecs"))
        end
      )

      assert %S3{access_key_id: "ecs_access_key_id", secret_access_key: "ecs_secret_access_key"} =
               with_env(
                 [TEST_AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: "/ecs_credential_path"],
                 fn -> %S3{} |> S3.Credentials.ensure_credentials() end
               )
    end

    test "handles cached_credentials" do
      update_cached_credentials!(%{
        access_key_id: "still_valid_id",
        secret_access_key: "still_valid_secret",
        expiration: NaiveDateTime.add(NaiveDateTime.utc_now(), 1, :hour)
      })

      assert %S3{access_key_id: "still_valid_id", secret_access_key: "still_valid_secret"} =
               %S3{} |> S3.Credentials.ensure_credentials()
    end

    test "handles expired credentials", %{bypass: bypass} do
      update_cached_credentials!(%{
        access_key_id: "expired_id",
        secret_access_key: "expired_secret",
        expiration: NaiveDateTime.add(NaiveDateTime.utc_now(), -1, :hour)
      })

      Bypass.expect(
        bypass,
        "GET",
        "/ecs/ecs_credential_path",
        fn conn ->
          conn
          |> Plug.Conn.put_resp_content_type("application/json")
          |> Plug.Conn.resp(200, credentials("new"))
        end
      )

      assert %S3{access_key_id: "new_access_key_id", secret_access_key: "new_secret_access_key"} =
               with_env(
                 [TEST_AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: "/ecs_credential_path"],
                 fn -> %S3{} |> S3.Credentials.ensure_credentials() end
               )
    end
  end

  defp credentials(prefix) do
    %{
      AccessKeyId: "#{prefix}_access_key_id",
      SecretAccessKey: "#{prefix}_secret_access_key",
      Expiration: NaiveDateTime.add(NaiveDateTime.utc_now(), 6, :hour)
    }
    |> Jason.encode!()
  end

  defp update_cached_credentials!(credentials) do
    send(Livebook.S3Credentials, {:credentials, credentials})
  end

  defp with_env(env_vars, fun) do
    existing =
      Enum.map(env_vars, fn {env, _value} ->
        {env, env |> to_string() |> System.get_env()}
      end)

    try do
      System.put_env(env_vars)
      fun.()
    after
      System.put_env(existing)
    end
  end
end
