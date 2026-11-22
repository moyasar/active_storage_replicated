module ActiveStorageReplicated
  class RedirectsController < ActiveStorage::BaseController
    def show
      url = Redirect.target_url(params[:token])
      if url.nil?
        head :not_found
        return
      end

      response.headers["Cache-Control"] = "no-store"
      redirect_to url, allow_other_host: true
    rescue ActiveStorage::FileNotFoundError
      head :not_found
    end
  end
end
