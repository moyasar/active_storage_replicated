Rails.application.routes.draw do
  scope ActiveStorage.routes_prefix do
    get "/replicated/:token" => "active_storage_replicated/redirects#show", as: :active_storage_replicated_redirect
  end
end
