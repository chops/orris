defmodule OrrisConsole.ErrorHTML do
  @moduledoc "One generic page per status; never a reason, path or identity."
  def render("404.html", _assigns), do: page("Not found", "The requested page is not available.")
  def render("413.html", _assigns), do: page("Too large", "The request was too large.")
  def render("429.html", _assigns), do: page("Try later", "Too many attempts.")
  def render(_template, _assigns), do: page("Unavailable", "The console could not complete the request.")

  defp page(title, text) do
    "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Orris console</title></head><body><main class=\"error\"><h1>#{title}</h1><p>#{text}</p></main></body></html>"
  end
end
