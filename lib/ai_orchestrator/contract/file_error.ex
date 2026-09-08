defmodule AiOrchestrator.Contract.FileError do
  @moduledoc """
  Closed file-error vocabulary used by adapter-boundary diagnostics.

  The list is explicit so an OTP update cannot silently widen persisted
  diagnostic data. A pinned-toolchain contract test detects drift against
  `:file.posix()` and requires an intentional review.
  """

  @errnos [
    :eacces,
    :eagain,
    :ebadf,
    :ebadmsg,
    :ebusy,
    :edeadlk,
    :edeadlock,
    :edquot,
    :eexist,
    :efault,
    :efbig,
    :eftype,
    :eintr,
    :einval,
    :eio,
    :eisdir,
    :eloop,
    :emfile,
    :emlink,
    :emultihop,
    :enametoolong,
    :enfile,
    :enobufs,
    :enodev,
    :enoent,
    :enolck,
    :enolink,
    :enomem,
    :enospc,
    :enosr,
    :enostr,
    :enosys,
    :enotblk,
    :enotdir,
    :enotsup,
    :enxio,
    :eopnotsupp,
    :eoverflow,
    :eperm,
    :epipe,
    :erange,
    :erofs,
    :espipe,
    :esrch,
    :estale,
    :etxtbsy,
    :exdev
  ]

  @spec errnos() :: [atom()]
  def errnos, do: @errnos
end
